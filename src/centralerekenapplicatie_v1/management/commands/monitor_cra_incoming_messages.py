from django.core.management.base import BaseCommand

from centralerekenapplicatie_v1.models import AreaMetric, LineMetric
from telcameras_v2.tools import get_messages_count_for_past_minutes


class Command(BaseCommand):
    help = "Monitor whether incoming messages are still arriving"
    def add_arguments(self, parser):
        parser.add_argument(
            'minutes',
            type=int,
            default=15,
            nargs="?",
            help="The number of minutes in the past which it checks for any available data."
        )

    def handle(self, *args, **options):
        minutes = options['minutes']

        message_count_area = get_messages_count_for_past_minutes(AreaMetric, 'timestamp', minutes)
        message_count_line = get_messages_count_for_past_minutes(LineMetric, 'timestamp', minutes)

        if not any([message_count_area, message_count_line]):
            error_message = f'Last CRA record was more than {minutes} minutes ago.'
            assert message_count_line, error_message
