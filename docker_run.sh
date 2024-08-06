#!/bin/bash


echo "Creating .env file..."
echo -e "AIRFLOW_UID=$(id -u)" > .env

echo "Initializing Airflow database..."
docker-compose up airflow-init

echo "Starting docker containers in detached mode "
docker-compose up -d --build

echo "Docker containers started successfully."
