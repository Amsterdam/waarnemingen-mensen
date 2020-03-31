from typing import NamedTuple

from django.conf import settings
from rest_framework.authentication import (TokenAuthentication,
                                           get_authorization_header)
from rest_framework.exceptions import AuthenticationFailed


class User(NamedTuple):
    is_authenticated: bool = False


class SimpleTokenAuthentication(TokenAuthentication):
    def authenticate_credentials(self, key):
        if not key == settings.AUTHORIZATION_TOKEN:
            raise AuthenticationFailed("Invalid token.")

        user = User(is_authenticated=True)
        return user, None
