import logging

from django.db import transaction, connection
from django.core.management.base import BaseCommand, CommandError
from django.contrib.auth.models import Group, User
from rest_framework.authtoken.models import Token

log = logging.getLogger(__name__)


@transaction.atomic
class Command(BaseCommand):
    help = 'Adds a user to the database and returns the auth token for it'

    def add_arguments(self, parser):
        parser.add_argument('username', type=str)
        parser.add_argument('group_name', type=str)

    def handle(self, *args, **options):
        try:
            group = Group.objects.get(name=options['group_name'])
        except Group.DoesNotExist:
            groups = [group.name for group in Group.objects.all()]
            raise CommandError(f"Group {options['group_name']} does not exist. "
                               f"It should be one of {groups}")

        user = User.objects.create_user(options['username'])
        user.groups.add(group)
        token = Token.objects.create(user=user)
        print("Token:", token.key)
