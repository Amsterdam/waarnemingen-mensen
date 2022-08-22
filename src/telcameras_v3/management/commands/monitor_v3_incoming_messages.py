from django.core.management.base import BaseCommand

from telcameras_v3.models import Observation
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
        error_message = f'Last telcameras_v3 record was more than {minutes} minutes ago.'
        message_count = get_messages_count_for_past_minutes(Observation, 'timestamp', minutes)
        assert message_count, error_message
