from datapunt_api.rest import DatapuntViewSetWritable

from .serializers import SensorSerializer


class RecordViewSet(DatapuntViewSetWritable):
    serializer_class = SensorSerializer
    serializer_detail_class = SensorSerializer

    http_method_names = ['post']
