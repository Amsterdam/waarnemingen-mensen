import json
import logging

from django.conf import settings
from ingress.consumer.base import BaseConsumer

from centralerekenapplicatie_v1.serializers import (AreaMetricSerializer,
                                                    CountMetricSerializer,
                                                    LineMetricSerializer)
from telcameras_v2.tools import SensorError, get_sensor_for_data

logger = logging.getLogger(__name__)


class MetricParser(BaseConsumer):
    collection_name = 'centralerekenapplicatie'

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
        record = json.loads(raw_data)
        
        # Convert source object to root values
        record['message_id'] = record.pop('id')
        source = record['source']
        record['sensor'] = source['sensor']
        record['timestamp'] = source['timestamp']
        record['original_id'] = source['originalId']
        record['admin_id'] = source['adminId']
        if record['type'] == 'countMetrics':
            record['interval'] = source['interval']
        del record['source']

        if record['type'] == 'areaMetrics':
            # CamelCase to snake_case
            record['total_distance'] = record['totalDistance']
            record['total_time'] = record['totalTime']

            serializer = AreaMetricSerializer(data=record)
            serializer.is_valid(raise_exception=True)

        elif record['type'] == 'lineMetrics':
            for count in record['counts']:
                count['line_metric_timestamp'] = record['timestamp']
            serializer = LineMetricSerializer(data=record)
            serializer.is_valid(raise_exception=True)

        elif record['type'] == 'countMetrics':
            serializer = CountMetricSerializer(data=record)
            serializer.is_valid(raise_exception=True)
            
        else:
            raise Exception("Unknown record type")

        return serializer.save()
