from django.conf import settings
from django.contrib.auth import get_user_model
from rest_framework.authentication import BaseAuthentication
from rest_framework.exceptions import PermissionDenied


class TokenAuthentication(BaseAuthentication):
    """
    Simple token based authentication.
    Clients should authenticate by passing the token key in the "Authorization"
    HTTP header, prepended with the string "Token ".  For example:
        Authorization: Token 401f7ac837da42b97f613d789819ff93537bee6a

    This is based on Django Rest Frameworks' TokenAuthentication, but instead
    we do not tie to a specific user.
    See https://www.django-rest-framework.org/api-guide/authentication/#tokenauthentication
    """

    def authenticate(self, request):
        if (
            request.META["HTTP_AUTHORIZATION"].replace("Token ", "")
            != settings.AUTHORIZATION_TOKEN
        ):
            raise PermissionDenied(detail="Invalid token.")

        UserModel = get_user_model()
        user = UserModel()

        return user, None
