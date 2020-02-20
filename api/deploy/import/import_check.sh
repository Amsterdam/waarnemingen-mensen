#!/bin/sh

set -e
set -u
set -x

DIR="$(dirname $0)"

dc() {
	docker-compose -p partition -f ${DIR}/docker-compose.yml $*
}

dc stop
dc rm --force
dc pull

dc run --rm waarnemingen-mensen python manage.py peoplemeasurement_timestamp_check
dc run --rm waarnemingen-mensen python manage.py passage_timestamp_check

dc stop

