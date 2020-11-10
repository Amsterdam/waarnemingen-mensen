import json
import logging

from django.conf import settings

from ingress.parser import IngressParser
from telcameras_v2.serializers import ObservationSerializer
from telcameras_v2.tools import (SensorError, data_to_observation,
                                 get_sensor_for_data)

logger = logging.getLogger(__name__)


class TelcameraParser(IngressParser):
    endpoint_url_key = 'telcameras_v2'

    def parse_single_message(self, ingress_raw_data):
        data = json.loads(ingress_raw_data)['data']
        observation = data_to_observation(data)

        if not settings.STORE_ALL_DATA:
            # Does the sensor exist and is it active
            try:
                # We're not actually doing anything with the sensor, but by getting it we just make
                # sure it exists and it's active
                sensor = get_sensor_for_data(observation)
            except SensorError as e:
                logger.info(str(e))
                # We don't want to store this message, but we don't want to throw an error either.
                # For that reason we simply return so that the parser will mark it as parsed successfully
                return

        observation_serializer = ObservationSerializer(data=observation)
        observation_serializer.is_valid(raise_exception=True)
        return observation_serializer.save()
