import logging
import sys

from datapunt_api.rest import DatapuntViewSetWritable
from rest_framework import exceptions, status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from .serializers import ObservationSerializer

logger = logging.getLogger(__name__)


class RecordViewSet(DatapuntViewSetWritable):
    serializer_class = ObservationSerializer
    serializer_detail_class = ObservationSerializer

    http_method_names = ['post']
    permission_classes = [IsAuthenticated]

    def create(self, request, *args, **kwargs):
        try:
            observation = request.data['data'][0]
            message = observation.pop('message')
            version = observation.pop('version')
            counts = []
            persons = []
            for obs in request.data['data']:
                if obs['message_type'] == 'count':
                    for count in obs['aggregate']:
                        count['external_id'] = count.pop('id')  # Count aggregates have an id, so to avoid colisions with the django orm id we rename the existing id that to "external_id"
                        count['message'] = message
                        count['version'] = version
                        if 'geom' in count:
                            count['geom'] = None if not count['geom'] else count['geom']
                        counts.append(count)
                elif obs['message_type'] == 'person':
                    for person in obs['aggregate']:
                        person['message'] = message
                        person['version'] = version
                        person['person_id'] = person.pop('personId')
                        if 'geom' in person:
                            person['geom'] = None if not person['geom'] else person['geom']
                        persons.append(person)

            observation['counts'] = counts
            observation['persons'] = persons

            del observation['message_type']
            del observation['aggregate']

            # Round lat/longs to 13 decimal places, because sometimes they are absurdly long
            observation['latitude'] = round(observation['latitude'], 13)
            observation['longitude'] = round(observation['longitude'], 13)

            observation_serializer = ObservationSerializer(data=observation)
            observation_serializer.is_valid(raise_exception=True)
            observation_serializer.save()

            return Response("", status=status.HTTP_201_CREATED)

        except (exceptions.ValidationError, KeyError, TypeError) as e:
            logger.error(e)
            return Response(str(e), status=status.HTTP_400_BAD_REQUEST)
