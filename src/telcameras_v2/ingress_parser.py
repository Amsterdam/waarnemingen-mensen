import json
from collections import namedtuple
from ingress.parser import IngressParser
from telcameras_v2.tools import data_to_observation, store_data_for_sensor
from telcameras_v2.serializers import ObservationSerializer


class TelcameraParser(IngressParser):
    endpoint_url_key = 'telcameras_v2'

    def parse_single_message(self, ingress_raw_data):
        data = json.loads(ingress_raw_data)['data']
        observation = data_to_observation(data)

        # Does the sensor exist and is it active
        store, message = store_data_for_sensor(observation)
        if not store:
            # We don't want to store this message, but we don't want to throw an error
            # For that reason we simply return a mocked model with an id so that the
            # parser will mark it as parsed succesfully
            return namedtuple('MockModel', 'id')(id=1)

        observation_serializer = ObservationSerializer(data=observation)
        observation_serializer.is_valid(raise_exception=True)
        return observation_serializer.save()
