from typing import NamedTuple

from django.conf import settings
from django.contrib.auth.models import Group, User
from rest_framework.authentication import TokenAuthentication
from rest_framework.authtoken.models import Token
from rest_framework.exceptions import AuthenticationFailed


class User(NamedTuple):
    is_authenticated: bool = False


class SimpleTokenAuthentication(TokenAuthentication):
    def authenticate_credentials(self, key):
        group = Group.objects.get(name=settings.GROUP_POST_DATA)
        tokens = Token.objects.filter(key=key)
        if tokens.count() == 1 and group in tokens[0].user.groups.all():
            user = User(is_authenticated=True)
            return user, None

        raise AuthenticationFailed("Invalid token.")


class SimpleGetTokenAuthentication(TokenAuthentication):
    def authenticate_credentials(self, key):
        group = Group.objects.get(name=settings.GROUP_GET_DATA)
        tokens = Token.objects.filter(key=key)
        if tokens.count() == 1 and group in tokens[0].user.groups.all():
            user = User(is_authenticated=True)
            return user, None

        raise AuthenticationFailed("Invalid token.")
