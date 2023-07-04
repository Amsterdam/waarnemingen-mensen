import json
import logging

from ingress.consumer.base import BaseConsumer

from telcameras_v3.serializers import ObservationSerializer

logger = logging.getLogger(__name__)


class TelcameraParser(BaseConsumer):
    collection_name = "telcameras_v3"

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
        observation = self.data_to_observation(raw_data)
        observation_serializer = ObservationSerializer(data=observation)
        observation_serializer.is_valid(raise_exception=True)
        return observation_serializer.save()

    def data_to_observation(self, raw_data: dict):
        raw_json = json.loads(raw_data)
        sensor_name = raw_json["sensor"]
        observation = dict(
            message_id=raw_json["id"],
            timestamp=raw_json["timestamp"],
            sensor=sensor_name,
            sensor_type=raw_json["sensor_type"],
            sensor_state=raw_json["status"],
            latitude=raw_json["latitude"],
            longitude=raw_json["longitude"],
            interval=raw_json["interval"],
            density=raw_json["density"],
            groupaggregates=[
                dict(
                    observation_timestamp=raw_json["timestamp"],
                    azimuth=aggregate["azimuth"],
                    count=aggregate["count"],
                    cumulative_distance=aggregate["cumulative_distance"],
                    cumulative_time=aggregate["cumulative_time"],
                    median_speed=aggregate["median_speed"],
                    count_scrambled=None,  # This field is filled by the serializer
                    persons=[
                        dict(
                            observation_timestamp=raw_json["timestamp"],
                            person_observation_timestamp=person[
                                "observation_timestamp"
                            ],
                            record=person["record"],
                            distance=person["distance"],
                            time=person["time"],
                            speed=person["speed"],
                            type=person["type"],
                        )
                        for person in aggregate.get("signals", [])
                    ],
                )
                for aggregate in raw_json["direction"]
            ],
        )
        return observation
