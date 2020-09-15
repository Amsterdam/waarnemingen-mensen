from django.contrib.auth.models import Group, User
from factory import fuzzy
from rest_framework.authtoken.models import Token


def create_auth_headers_by_group_name(group_name):
    group = Group.objects.get(name=group_name)
    user = User.objects.create_user(fuzzy.FuzzyText().fuzz().lower(), password='foobar')
    user.groups.add(group)
    token = Token.objects.create(user=user)
    return {'HTTP_AUTHORIZATION': f"Token {token.key}"}
