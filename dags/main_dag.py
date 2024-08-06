from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.discord.operators.discord_webhook import DiscordWebhookOperator


from airflow.utils.dates import days_ago

from scripts.create_topic import kafka_create_topic_main
from scripts.producer import kafka_producer_main
from scripts.consumer_cassandra import kafka_consumer_cassandra_main
from scripts.consumer_mongo import kafka_consumer_mongodb_main
from scripts.check_cassandra import check_cassandra_main
from scripts.check_mongo import check_mongodb_main


default_args = {
    'owner': 'airflow',
}

def fetch_email_and_otp(**context):
    ti = context['ti']
    email = ti.xcom_pull(task_ids='kafka_producer', key='email')
    
    if not email:
        raise ValueError("No email found in XCom.")
    
    email = email.strip()
    otp_data1 = check_cassandra_main(email)
    otp_data2 = check_mongodb_main(email)
    
    return email, otp_data1.get('otp', ''), otp_data2.get('otp', '')

with DAG('main_dag', default_args=default_args, schedule_interval=None, catchup=False) as dag:

    create_new_topic = PythonOperator(
        task_id='create_new_topic',
        python_callable=kafka_create_topic_main,
        retries=2,
        retry_delay=timedelta(seconds=5),
        execution_timeout=timedelta(seconds=10)
    )

    kafka_producer = PythonOperator(
        task_id='kafka_producer',
        python_callable=kafka_producer_main,
        retries=2,
        retry_delay=timedelta(seconds=5),
        execution_timeout=timedelta(seconds=30)
    )

    kafka_consumer_cassandra = PythonOperator(
        task_id='kafka_consumer_cassandra',
        python_callable=kafka_consumer_cassandra_main,
        retries=2,
        retry_delay=timedelta(seconds=5),
        execution_timeout=timedelta(seconds=45)
    )
    kafka_consumer_mongodb = PythonOperator(
        task_id='kafka_consumer_mongodb', 
        python_callable=kafka_consumer_mongodb_main,
        retries=2, 
        retry_delay=timedelta(seconds=5),
        execution_timeout=timedelta(seconds=45)
    )

    check_cassandra = PythonOperator(
        task_id='check_cassandra',
        python_callable=fetch_email_and_otp,
        retries=2,
        retry_delay=timedelta(seconds=5),
        execution_timeout=timedelta(seconds=30)
    )

    check_mongodb = PythonOperator(
        task_id='check_mongodb',
        python_callable=fetch_email_and_otp,
        retries=2, 
        retry_delay=timedelta(seconds=5),
        execution_timeout=timedelta(seconds=30)
    )

    send_email_cassandra = EmailOperator(
        task_id='send_email_cassandra',
        to="{{ task_instance.xcom_pull(task_ids='check_cassandra')[0] }}",
        subject="OTP from Cassandra : {{ task_instance.xcom_pull(task_ids='check_cassandra')[1] }}",
        html_content="""<html>
            <body>
            <h1>Your One-Time-Password from Cassandra</h1>
            <p>{{ task_instance.xcom_pull(task_ids='check_cassandra')[1] }}</p>
            </body>
            </html>""",
    )

    send_email_mongodb = EmailOperator(
        task_id='send_email_mongodb',
        conn_id='smtp_email',
        to="{{ task_instance.xcom_pull(task_ids='check_mongodb')[0] }}",
        subject="OTP from MongoDB : {{ task_instance.xcom_pull(task_ids='check_mongodb')[1] }}",
        html_content="""<html>
            <body>
            <h1>Your One-Time-Password from MongoDB</h1>
            <p>{{ task_instance.xcom_pull(task_ids='check_mongodb')[1] }}</p>
            </body>
            </html>""",
    )

    send_slack_cassandra = SlackWebhookOperator(
        task_id='send_slack_cassandra',
        slack_webhook_conn_id = 'slack_webhook',
        message="""\
            :red_circle: New e-mail and OTP arrival from Cassandra
            :email: -> {{ task_instance.xcom_pull(task_ids='check_cassandra')[0] }}
            :ninja: -> {{ task_instance.xcom_pull(task_ids='check_cassandra')[1] }}
            """,
        channel='#data-engineering',
        username='airflow'
    )

    send_discord_mongodb = DiscordWebhookOperator(
        task_id='send_discord_mongodb',
        http_conn_id='discord_webhook',
        message="""
        🔴 New e-mail and OTP arrival from MongoDB
        📧 -> {{ task_instance.xcom_pull(task_ids='check_mongodb')[0] }}
        🥷 -> {{ task_instance.xcom_pull(task_ids='check_mongodb')[1] }}
        """,
    )

    create_new_topic >> kafka_producer >> [kafka_consumer_cassandra, kafka_consumer_mongodb]
    
    kafka_consumer_cassandra >> check_cassandra >> [send_email_cassandra, send_slack_cassandra]
    kafka_consumer_mongodb >> check_mongodb >> [send_email_mongodb, send_discord_mongodb]