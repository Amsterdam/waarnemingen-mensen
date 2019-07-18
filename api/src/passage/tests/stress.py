#!/usr/bin/env python3
"""
There were some problems in production under load. This file tests some simple
stress, but can definitely be expanded.
"""
import requests
import argparse
import sys
import os
import uuid
from datetime import datetime
from multiprocessing import Process


URL = 'https://acc.waarnemingen.amsterdam.nl/v0/milieuzone/passage/'

POST_HEADERS = {
        'Accept': 'application/json',
        'Content-type': 'application/json'
    }


def generate_request(id):
    tmp = {
        "version": "passage-v1",
        "id": str(uuid.uuid4()),
        "passage_at": str(datetime.now()),
        "straat": "Spaarndammerdijk",
        "rijstrook": 1,
        "rijrichting": 1,
        "camera_id": "ddddffff-4444-aaaa-7777-aaaaeeee1111",
        "camera_naam": "Spaarndammerdijk [Z]",
        "camera_kijkrichting": 0,
        "camera_locatie": {
            "type": "Point",
            "coordinates": [
                4.845423,
                52.386831
            ]
        },
        "kenteken_land": "NL",
        "kenteken_nummer_betrouwbaarheid": 640,
        "kenteken_land_betrouwbaarheid": 690,
        "kenteken_karakters_betrouwbaarheid": [
            {
                "betrouwbaarheid": 650,
                "positie": 1
            },
            {
                "betrouwbaarheid": 630,
                "positie": 2
            },
            {
                "betrouwbaarheid": 640,
                "positie": 3
            },
            {
                "betrouwbaarheid": 660,
                "positie": 4
            },
            {
                "betrouwbaarheid": 620,
                "positie": 5
            },
            {
                "betrouwbaarheid": 640,
                "positie": 6
            }
        ],
        "indicatie_snelheid": 23,
        "automatisch_verwerkbaar": True,
        "voertuig_soort": "Bromfiets",
        "merk": "SYM",
        "inrichting": "N.V.t.",
        "datum_eerste_toelating": "2015-03-06",
        "datum_tenaamstelling": "2015-03-06",
        "toegestane_maximum_massa_voertuig": 249,
        "europese_voertuigcategorie": "L1",
        "europese_voertuigcategorie_toevoeging": "e",
        "taxi_indicator": True,
        "maximale_constructie_snelheid_bromsnorfiets": 25,
        "brandstoffen": [
            {
                "brandstof": "Benzine",
                "volgnr": 1
            }
        ],
        "versit_klasse": "test klasse"
    }
    return tmp


def simulate_request(start, blocksize):
    pid = os.getpid()
    print(f'{pid} start: {start} block: {blocksize}')
    # session = requests.Session()
    for i in range(blocksize):
        id = start + i
        try:
            # resp = session.post(
            resp = requests.post(
                URL,
                json=generate_request(id),
                headers=POST_HEADERS
            )
            # print(f'{pid} HTTP response code: {resp.status_code}')
            # if record could not be created, dump response
            if resp.status_code != 201:
                print(resp.content)
        except Exception as e:
            print(e)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("workers", help="# workers", type=int)
    parser.add_argument("requests", help="# request", type=int)
    if len(sys.argv) == 1:
        parser.print_help()
        return
    args = parser.parse_args()
    procs = []
    blocksize = int(args.requests / args.workers)
    for i in [x * blocksize for x in range(args.workers)]:
        p = Process(target=simulate_request, args=(i, blocksize,))
        p.start()
        procs.append(p)

    print('Waiting for workers to complete')
    for p in procs:
        p.join()


if __name__ == '__main__':
    main()
