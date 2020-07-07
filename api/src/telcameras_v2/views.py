from datapunt_api.rest import DatapuntViewSetWritable
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework import status
from .serializers import ObservationSerializer


class RecordViewSet(DatapuntViewSetWritable):
    serializer_class = ObservationSerializer
    serializer_detail_class = ObservationSerializer

    http_method_names = ['post']
    permission_classes = [IsAuthenticated]
