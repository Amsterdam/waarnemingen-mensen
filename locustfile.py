"""
This is a (much needed) load test for this repo. These are some example usages:

locust --host=http://127.0.0.1:8001 --no-web -c 250 -r 25 --run-time 1m PeopleUser
locust --host=http://127.0.0.1:8001 --no-web -c 250 -r 25 --run-time 1m CarsUser
locust --host=http://127.0.0.1:8001 --no-web -c 250 -r 25 --run-time 30s
"""
from uuid import uuid4
from locust import HttpLocust, TaskSet, task


PASSAGE_ENDPOINT_URL = "/v0/milieuzone/passage/"
PEOPLE_MEASUREMENT_ENDPOINT_URL = "/v0/people/measurement/"


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
                "timestamp": "2018-12-07T19:40+01:00",
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
            "passage_at": "2019-08-06T09:16:55+02:00",
            "created_at": "2019-08-06T09:17:04.507910+02:00",
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
            "merk": "DAEWOO",
            "inrichting": "stationwagen",
            "datum_eerste_toelating": "2004-11-01",
            "datum_tenaamstelling": "2004-11-01",
            "toegestane_maximum_massa_voertuig": 1828,
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


class CarsUser(HttpLocust):
    task_set = CarsBehaviour
    weight = 3
