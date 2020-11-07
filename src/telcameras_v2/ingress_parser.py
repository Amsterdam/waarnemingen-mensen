import json

from ingress.parser import IngressParser
from telcameras_v2.data_conversions import data_to_observation
from telcameras_v2.serializers import ObservationSerializer


class TelcameraParser(IngressParser):
    endpoint_url_key = 'telcameras_v2'

    def parse_single_message(self, ingress_raw_data):
        data = json.loads(json.loads(ingress_raw_data))['data']
        observation = data_to_observation(data)

        observation_serializer = ObservationSerializer(data=observation)
        observation_serializer.is_valid(raise_exception=True)
        return observation_serializer.save()
