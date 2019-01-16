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

dc run --rm iotsignals python make_paritions.py

dc stop
