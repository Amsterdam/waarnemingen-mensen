import logging
from random import randint

from peoplemeasurement.models import Sensors

logger = logging.getLogger(__name__)


def data_to_observation(data):
    """
    This endpoint receives data in a format which does not match the database layout. This function
    transforms its structure to match the database.
    """
    observation = data[0]
    message = observation.pop('message')
    version = observation.pop('version')
    counts = []
    persons = []
    for obs in data:
        if obs['message_type'] == 'count':
            for count in obs['aggregate']:
                count['external_id'] = count.pop(
                    'id')  # Count aggregates have an id, so to avoid colisions with the django orm id we rename the existing id that to "external_id"
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

    return observation


class SensorError(Exception):
    pass


def get_sensor_for_data(data):
    # Does the sensor exist and is it active
    try:
        sensor = Sensors.objects.get(objectnummer=data.get('sensor', ''))
    except Sensors.DoesNotExist:
        raise SensorError(f"The sensor '{data['sensor']}' was not found, so the data is not stored.")
    if not sensor.is_active:
        raise SensorError(f"The sensor '{data['sensor']}' exists but is not active.")
    return sensor


def scramble_count_aggregate(count_aggregate):
    """
    For privacy reasons we need a count which is slightly scrambled. We therefore add or subtract 1, or do nothing.
    The reason is that if there is a count of 1 and you have information from several cameras, you could
    "follow" one person through the city.
    """

    # Since this is not meant to be cryptographically secure we simply use the random module
    if count_aggregate.count_in is not None and count_aggregate.count_in_scrambled is None:
        if count_aggregate.count_in == 0:
            count_aggregate.count_in_scrambled = count_aggregate.count_in + randint(0, 1)
        else:
            count_aggregate.count_in_scrambled = count_aggregate.count_in + randint(-1, 1)
    if count_aggregate.count_out is not None and count_aggregate.count_out_scrambled is None:
        if count_aggregate.count_out < 0:
            count_aggregate.count_out_scrambled = count_aggregate.count_out + randint(0, 1)
        else:
            count_aggregate.count_out_scrambled = count_aggregate.count_out + randint(-1, 1)

    return count_aggregate
