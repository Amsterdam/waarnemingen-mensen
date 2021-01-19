import json
import logging

from django.conf import settings

from centralerekenapplicatie_v1.models import CraCount, CraModel
from centralerekenapplicatie_v1.serializers import CraSerializer
from ingress.parser import IngressParser
from telcameras_v2.tools import SensorError, get_sensor_for_data

logger = logging.getLogger(__name__)


class CraParser(IngressParser):
    endpoint_url_key = 'centralerekenapplicatie'

    def parse_single_message(self, ingress_raw_data):
        record = json.loads(ingress_raw_data)
        record['message_id'] = record.pop('id')
        record['sensor'] = record['source']['sensor']
        record['timestamp'] = record['source']['timestamp']
        record['original_id'] = record['source']['originalId']
        record['admin_id'] = record['source']['adminId']
        del record['source']

        if 'count' in record:
            record['counts'] = [{'count': record['count']}]

        if not settings.STORE_ALL_DATA_CRA:
            # Does the sensor exist and is it active
            try:
                # We're not actually doing anything with the sensor, but by getting it we just make
                # sure it exists and it's active
                sensor = get_sensor_for_data(record)
            except SensorError as e:
                logger.info(str(e))
                # We don't want to store this message, but we don't want to throw an error either.
                # For that reason we simply return so that the parser will mark it as parsed successfully
                return

        cra_serializer = CraSerializer(data=record)
        cra_serializer.is_valid(raise_exception=True)
        return cra_serializer.save()
