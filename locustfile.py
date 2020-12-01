"""
This is a (much needed) load test for this repo. These are some example usages:

locust --host=http://127.0.0.1:8001 --headless --users 250 --hatch-rate 25 --run-time 1m PeopleUser
locust --host=http://127.0.0.1:8001 --headless --users 250 --hatch-rate 25 --run-time 30s
"""
import datetime
import time
from uuid import uuid4

from locust import HttpUser, between, task

AUTHORIZATION_HEADER = {'Authorization': f"Token {os.environ['AUTHORIZATION_TOKEN']}"}
PEOPLE_MEASUREMENT_ENDPOINT_URL = "/telcameras/v1/"


def get_dt_with_tz_info():
    # Calculate the offset taking into account daylight saving time
    utc_offset_sec = time.altzone if time.localtime().tm_isdst else time.timezone
    utc_offset = datetime.timedelta(seconds=-utc_offset_sec)
    return datetime.datetime.now().replace(tzinfo=datetime.timezone(offset=utc_offset)).isoformat()


def create_message():
    return {
        "data": {
            "id": str(uuid4()),
            "density": "0.0",
            "sensortype": "countingcamera",
            "latitude": "52.3709",
            "count": "6.0",
            "sensor": "TEST",
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


class PeopleUser(HttpUser):
    weight = 1
    wait_time = between(0, 1)

    @task(1)
    def post_people(self):
        self.client.post(PEOPLE_MEASUREMENT_ENDPOINT_URL, json=create_message(), headers=AUTHORIZATION_HEADER)
