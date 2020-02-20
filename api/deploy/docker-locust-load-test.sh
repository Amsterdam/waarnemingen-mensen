#!/usr/bin/env bash

set -u   # crash on missing env variables
set -e   # stop on any error
set -x   # print what we are doing

# Remove docker containers from previous runs
docker-compose -p mensen_load down
docker-compose -p mensen_load rm -f

# Run migrations
docker-compose -p mensen_load run api /deploy/docker-wait.sh
docker-compose -p mensen_load run api /deploy/docker-migrate.sh

# Run the load test
docker-compose -p mensen_load up locust

# Remove remaining docker containers
docker-compose -p mensen_load down
docker-compose -p mensen_load rm -f
