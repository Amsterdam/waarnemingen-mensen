import logging
from random import randint

from peoplemeasurement.models import Sensors

logger = logging.getLogger(__name__)


class SensorError(Exception):
    pass


def get_sensor_for_data(data):
    # Does the sensor exist and is drop_incoming_data set to False
    try:
        sensor = Sensors.objects.get(objectnummer=data.get("sensor", ""))
    except Sensors.DoesNotExist:
        raise SensorError(
            f"The sensor '{data['sensor']}' was not found, so the data is not stored."
        )
    if sensor.drop_incoming_data:
        raise SensorError(
            f"The sensor '{data['sensor']}' exists but drop_incoming_data is set to True."
        )
    return sensor


def scramble_count_aggregate(count_aggregate):
    """
    For privacy reasons we need a count which is slightly scrambled. We therefore add or subtract 1, or do nothing.
    The reason is that if there is a count of 1 and you have information from several cameras, you could
    "follow" one person through the city.
    """

    # Since this is not meant to be cryptographically secure we simply use the random module
    if (
        count_aggregate.count_in is not None
        and count_aggregate.count_in_scrambled is None
    ):
        if count_aggregate.count_in == 0:
            count_aggregate.count_in_scrambled = count_aggregate.count_in + randint(
                0, 1
            )
        else:
            count_aggregate.count_in_scrambled = count_aggregate.count_in + randint(
                -1, 1
            )

    if (
        count_aggregate.count_out is not None
        and count_aggregate.count_out_scrambled is None
    ):
        if count_aggregate.count_out == 0:
            count_aggregate.count_out_scrambled = count_aggregate.count_out + randint(
                0, 1
            )
        else:
            count_aggregate.count_out_scrambled = count_aggregate.count_out + randint(
                -1, 1
            )

    if count_aggregate.count is not None and count_aggregate.count_scrambled is None:
        if count_aggregate.count == 0:
            count_aggregate.count_scrambled = count_aggregate.count + randint(0, 1)
        else:
            count_aggregate.count_scrambled = count_aggregate.count + randint(-1, 1)

    return count_aggregate
