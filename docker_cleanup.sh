#!/bin/bash

echo "Stopping all running containers..."
docker compose down

echo "Removing all stopped containers..."
docker container prune -f

echo "Removing all volumes..."
docker volume rm $(docker volume ls -q)

echo "Removing all build cache..."
docker builder prune -f

echo "removing airflow logs..."
rm -rf logs/*

echo "removing plugins/scripts/__pycache__..."
rm -rf plugins/scripts/__pycache__/*

echo "removing dags/__pycache__..."
rm -rf dags/__pycache__/*



echo "Docker cleanup completed, retaining all images."