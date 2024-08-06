# Use the Apache Airflow 2.9.3 image as the base image
FROM apache/airflow:2.9.3

# Switch to the "airflow" user
USER airflow

# Install pip globally
RUN curl -O 'https://bootstrap.pypa.io/get-pip.py' && \
    python3 get-pip.py

## Install libraries from requirements.txt
COPY requirements.txt /requirements.txt
COPY email_list.txt /email_list.txt

RUN pip install -r /requirements.txt
