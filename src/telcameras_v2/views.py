import logging

from datapunt_api.rest import DatapuntViewSetWritable
from django.conf import settings
from rest_framework import exceptions, status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from telcameras_v2.serializers import ObservationSerializer
from telcameras_v2.tools import (SensorError, data_to_observation,
                                 get_sensor_for_data)

logger = logging.getLogger(__name__)


class RecordViewSet(DatapuntViewSetWritable):
    serializer_class = ObservationSerializer
    serializer_detail_class = ObservationSerializer

    http_method_names = ['post']
    permission_classes = [IsAuthenticated]

    def create(self, request, *args, **kwargs):
        try:
            # Convert the data to a usable format
            data = request.data['data']
            observation = data_to_observation(data)

            if not settings.STORE_ALL_DATA_TELCAMERAS_V2:
                # Does the sensor exist and is it active
                try:
                    # We're not actually doing anything with the sensor, but by getting it we just make
                    # sure it exists and it's active
                    sensor = get_sensor_for_data(observation)
                except SensorError as e:
                    logger.info(str(e))
                    # We don't want to store this message, but we don't want to throw an error either.
                    # For that reason we simply return so that the parser will mark it as parsed successfully
                    return Response(str(e), status=status.HTTP_200_OK)

            # Serialize and store the data
            observation_serializer = ObservationSerializer(data=observation)
            observation_serializer.is_valid(raise_exception=True)
            observation_serializer.save()

            return Response("", status=status.HTTP_201_CREATED)

        except (exceptions.ValidationError, KeyError, TypeError) as e:
            logger.error(e)
            return Response(str(e), status=status.HTTP_400_BAD_REQUEST)
