import logging

from datapunt_api.rest import DatapuntViewSetWritable
from rest_framework import exceptions, status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from .data_conversions import data_to_observation
from .serializers import ObservationSerializer

logger = logging.getLogger(__name__)


class RecordViewSet(DatapuntViewSetWritable):
    serializer_class = ObservationSerializer
    serializer_detail_class = ObservationSerializer

    http_method_names = ['post']
    permission_classes = [IsAuthenticated]

    def create(self, request, *args, **kwargs):
        try:
            data = request.data['data']
            observation = data_to_observation(data)

            observation_serializer = ObservationSerializer(data=observation)
            observation_serializer.is_valid(raise_exception=True)
            observation_serializer.save()

            return Response("", status=status.HTTP_201_CREATED)

        except (exceptions.ValidationError, KeyError, TypeError) as e:
            logger.error(e)
            return Response(str(e), status=status.HTTP_400_BAD_REQUEST)
