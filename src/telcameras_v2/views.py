import logging

from datapunt_api.rest import DatapuntViewSetWritable
from rest_framework import exceptions, status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from peoplemeasurement.models import Sensors
from telcameras_v2.serializers import ObservationSerializer
from telcameras_v2.tools import data_to_observation, store_data_for_sensor

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

            # Does the sensor exist and is it active
            store, message = store_data_for_sensor(observation)
            if not store:
                return Response(message, status=status.HTTP_200_OK)

            # Serialize and store the data
            observation_serializer = ObservationSerializer(data=observation)
            observation_serializer.is_valid(raise_exception=True)
            observation_serializer.save()

            return Response("", status=status.HTTP_201_CREATED)

        except (exceptions.ValidationError, KeyError, TypeError) as e:
            logger.error(e)
            return Response(str(e), status=status.HTTP_400_BAD_REQUEST)
