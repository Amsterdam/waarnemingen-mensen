"""
This is a (much needed) load test for this repo. These are some example usages:

locust --host=http://127.0.0.1:8001 --no-web -c 250 -r 25 --run-time 1m PeopleUser
locust --host=http://127.0.0.1:8001 --no-web -c 250 -r 25 --run-time 1m CarsUser
locust --host=http://127.0.0.1:8001 --no-web -c 250 -r 25 --run-time 30s
"""
from uuid import uuid4
import datetime, time
from locust import HttpLocust, TaskSet, task, between


PASSAGE_ENDPOINT_URL = "/v0/milieuzone/passage/"
PEOPLE_MEASUREMENT_ENDPOINT_URL = "/v0/people/measurement/"


def get_dt_with_tz_info():
    # Calculate the offset taking into account daylight saving time
    utc_offset_sec = time.altzone if time.localtime().tm_isdst else time.timezone
    utc_offset = datetime.timedelta(seconds=-utc_offset_sec)
    return datetime.datetime.now().replace(tzinfo=datetime.timezone(offset=utc_offset)).isoformat()


def create_message(type_):
    if type_ == 'people':
        return {
            "data": {
                "id": str(uuid4()),
                "density": "0.0",
                "sensortype": "countingcamera",
                "latitude": "52.3709",
                "count": "6.0",
                "sensor": "GKS-1-4",
        
                "version": "1",
                "speed": "0.0",
                "timestamp": get_dt_with_tz_info(),
                "longitude": "4.89175"
            },
            "details": [
                {
                    "count": "2",
                    "id": "315fc896-82dd-46ee-ac91-31e1a1b1807a",
                    "direction": "down"
                },
                {
                    "count": "4",
                    "id": "a70b424f-4144-437b-a990-cbac421a4830",
                    "direction": "down"
                }
            ]
        }
    elif type_ == 'cars':
        return {
            "id": str(uuid4()),
            "passage_at": get_dt_with_tz_info(),
            "created_at": get_dt_with_tz_info(),
            "version": "1",
            "straat": None,
            "rijrichting": 1,
            "rijstrook": 2,
            "camera_id": "00856ef3-c6f5-4194-9531-a3267839674a",
            "camera_naam": "Muntbergweg (s111) nabij afrit (A9) uit oost - Rijstrook 2",
            "camera_kijkrichting": 337.5,
            "camera_locatie": {
                "type": "Point",
                "coordinates": [
                    4.945936,
                    52.301221
                ]
            },
            "kenteken_land": "NL",
            "kenteken_nummer_betrouwbaarheid": 990,
            "kenteken_land_betrouwbaarheid": 0,
            "kenteken_karakters_betrouwbaarheid": None,
            "indicatie_snelheid": None,
            "automatisch_verwerkbaar": None,
            "voertuig_soort": "Personenauto",
            "merk": "SPYKER",
            "inrichting": "stationwagen",
            "datum_eerste_toelating": "2001-01-01",
            "datum_tenaamstelling": "2001-01-02",
            "toegestane_maximum_massa_voertuig": 1234,
            "europese_voertuigcategorie": "M1",
            "europese_voertuigcategorie_toevoeging": None,
            "taxi_indicator": False,
            "maximale_constructie_snelheid_bromsnorfiets": None,
            "brandstoffen": [
                {
                    "volgnr": 1,
                    "brandstof": "Benzine",
                    "euroklasse": "Euro 3"
                }
            ],
            "extra_data": None,
            "diesel": None,
            "gasoline": None,
            "electric": None,
            "versit_klasse": "LPABEUR3"
        }


class PeopleBehaviour(TaskSet):
    @task(1)
    def post_people(self):
        self.client.post(PEOPLE_MEASUREMENT_ENDPOINT_URL, json=create_message('people'))


class CarsBehaviour(TaskSet):
    @task(1)
    def post_cars(self):
        self.client.post(PASSAGE_ENDPOINT_URL, json=create_message('cars'))


class PeopleUser(HttpLocust):
    task_set = PeopleBehaviour
    weight = 1
    wait_time = between(0, 1)


class CarsUser(HttpLocust):
    task_set = CarsBehaviour
    weight = 1
    wait_time = between(0, 1)
