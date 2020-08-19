from typing import NamedTuple

from django.conf import settings
from rest_framework import permissions
from rest_framework.exceptions import AuthenticationFailed


class User(NamedTuple):
    is_authenticated: bool = False

class SimpleGetTokenAuthentication(permissions.BasePermission):
    def has_permission(self, request, view):
        if not request.META.get('Authorization') == f'Token {settings.GET_AUTHORIZATION_TOKEN}':
            raise AuthenticationFailed("Invalid token.")

        user = User(is_authenticated=True)
        return user, None
