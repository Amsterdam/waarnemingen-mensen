import json
import logging

from django.conf import settings
from ingress.consumer.base import BaseConsumer

from telcameras_v2.tools import SensorError, get_sensor_for_data
from telcameras_v3.serializers import ObservationSerializer

logger = logging.getLogger(__name__)


class TelcameraParser(BaseConsumer):
    collection_name = 'telcameras_v3'

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
        observation = json.loads(raw_data)
        observation['message_id'] = observation.pop('id')
        observation['sensor_state'] = observation.pop('status')
        observation['groupaggregates'] = observation.pop('direction')
        for groupaggregate in observation['groupaggregates']:
            groupaggregate['observation_timestamp'] = observation['timestamp']
            groupaggregate['persons'] = groupaggregate.pop('signals', [])
            for person in groupaggregate['persons']:
                person['person_observation_timestamp'] = person.pop('observation_timestamp')
                person['observation_timestamp'] = observation['timestamp']

        observation_serializer = ObservationSerializer(data=observation)
        observation_serializer.is_valid(raise_exception=True)
        return observation_serializer.save()
