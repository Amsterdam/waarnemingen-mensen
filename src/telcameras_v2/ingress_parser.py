import json
import logging

from ingress.consumer.base import BaseConsumer
from telcameras_v2.serializers import ObservationSerializer

logger = logging.getLogger(__name__)


class TelcameraParser(BaseConsumer):
    collection_name = "telcameras_v2"

    """
    Whether or not to immediately remove messages once consumption succeeds.
    If set to False, message.consume_succeeded_at will be set.
    """
    remove_message_on_consumed = False

    """
    Whether or not to set Message.consume_started_at immediately once consumption starts
    """
    set_consume_started_at = True

    def get_default_batch_size(self):
        return 10000

    def consume_raw_data(self, raw_data):
        data = json.loads(raw_data)["data"]
        observation = self.data_to_observation(data)

        observation_serializer = ObservationSerializer(data=observation)
        observation_serializer.is_valid(raise_exception=True)
        return observation_serializer.save()

    def data_to_observation(self, data):
        """
        This endpoint receives data in a format which does not match the database layout. This function
        transforms its structure to match the database.
        """
        raw_json = data[0]
        sensor_name = raw_json["sensor"]
        message = raw_json["message"]
        version = raw_json["version"]

        observation = dict(
            sensor_name=sensor_name,
            sensor_type=raw_json["sensor_type"],
            sensor_state=raw_json["sensor_state"],
            # sensor=sensor_name,
            owner=raw_json["owner"],
            supplier=raw_json["supplier"],
            purpose=raw_json["purpose"],
            latitude=round(raw_json["latitude"], 13),
            longitude=round(raw_json["longitude"], 13),
            interval=raw_json["interval"],
            timestamp_message=raw_json["timestamp_message"],
            timestamp_start=raw_json["timestamp_start"],
            counts=[
                dict(
                    external_id=count["id"],
                    message=message,
                    version=version,
                    type=count["type"],
                    geom=None if not count.get("geom") else count.get("geom"),
                    # line=count['id'] if count["type"] == 'line' else None,
                    # area=count['id'] if count["type"] == 'zone' else None,
                    # For a type "line"
                    azimuth=count.get("azimuth"),
                    count_in=count.get("count_in"),
                    count_out=count.get("count_out"),
                    count_in_scrambled=None,  # This field is filled by the serializer
                    count_out_scrambled=None,  # This field is filled by the serializer
                    # for a type "zone"
                    area_size=count.get("area"),
                    count=count.get("count"),
                    count_scrambled=None,  # This field is filled by the serializer
                )
                for obs in data
                for count in obs["aggregate"]
                if obs["message_type"] == "count"
            ],
            persons=[
                dict(
                    message=message,
                    version=version,
                    geom=None if not person.get("geom") else person.get("geom"),
                    person_id=person["personId"],
                    observation_timestamp=person["observation_timestamp"],
                    record=person["record"],
                    speed=person["speed"],
                    quality=person["quality"],
                    distances=person.get("distances"),
                )
                for obs in data
                for person in obs["aggregate"]
                if obs["message_type"] == "person"
            ],
        )

        return observation
