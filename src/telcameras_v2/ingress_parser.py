import json
import logging

from django.conf import settings
from ingress.consumer.base import BaseConsumer

from telcameras_v2.serializers import ObservationSerializer
from telcameras_v2.tools import (SensorError, data_to_observation,
                                 get_sensor_for_data)

logger = logging.getLogger(__name__)


class TelcameraParser(BaseConsumer):
    collection_name = 'telcameras_v2'

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
        data = json.loads(raw_data)['data']
        observation = data_to_observation(data)

        observation_serializer = ObservationSerializer(data=observation)
        observation_serializer.is_valid(raise_exception=True)
        return observation_serializer.save()
