from django.contrib.postgres.fields import JSONField
from django.db import models


class JsonDump(models.Model):
    dump = JSONField()
