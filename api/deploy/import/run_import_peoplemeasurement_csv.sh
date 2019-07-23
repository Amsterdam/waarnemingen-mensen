#!/bin/sh

set -e
set -u
set -x

DIR="$(dirname $0)"

dc() {
	docker-compose -p import_peoplemeasurement_csv -f ${DIR}/docker-compose.yml $*
}

dc stop
dc rm --force
dc pull

dc run --rm iotsignals python manage.py import_peoplemeasurement_csv

dc stop

