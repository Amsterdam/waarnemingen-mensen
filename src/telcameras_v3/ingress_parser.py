import json
import logging

from django.conf import settings

from ingress.parser import IngressParser
from telcameras_v2.tools import SensorError, get_sensor_for_data
from telcameras_v3.serializers import ObservationSerializer

logger = logging.getLogger(__name__)


class TelcameraParser(IngressParser):
    endpoint_url_key = 'telcameras_v3'

    def parse_single_message(self, ingress_raw_data):
        observation = json.loads(ingress_raw_data)
        observation['message_id'] = observation.pop('id')
        observation['sensor_state'] = observation.pop('status')
        observation['groupaggregates'] = observation.pop('direction')
        for groupaggregate in observation['groupaggregates']:
            groupaggregate['observation_timestamp'] = observation['timestamp']
            groupaggregate['persons'] = groupaggregate.pop('signals', [])
            for person in groupaggregate['persons']:
                person['person_observation_timestamp'] = person.pop('observation_timestamp')
                person['observation_timestamp'] = observation['timestamp']

        if not settings.STORE_ALL_DATA_TELCAMERAS_V3:
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
