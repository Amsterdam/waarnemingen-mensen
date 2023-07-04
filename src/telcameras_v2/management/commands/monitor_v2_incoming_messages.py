from datetime import timedelta

from django.core.management.base import BaseCommand
from django.utils.timezone import now

from telcameras_v2.models import Observation


def get_messages_count_for_past_minutes(minutes):
    check_from = now() - timedelta(minutes=minutes)
    return Observation.objects.filter(timestamp_message__gte=check_from).count()


class Command(BaseCommand):
    help = "Monitor whether incoming messages are still arriving"

    def add_arguments(self, parser):
        parser.add_argument(
            "minutes",
            type=int,
            default=15,
            nargs="?",
            help="The number of minutes in the past which it checks for any available data.",
        )

    def handle(self, *args, **options):
        minutes = options["minutes"]
        error_message = (
            f"Last telcameras_v2 record was more than {minutes} minutes ago."
        )
        assert get_messages_count_for_past_minutes(minutes), error_message
