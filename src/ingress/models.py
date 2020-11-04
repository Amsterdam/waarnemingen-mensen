from django.db import models


class IngressQueue(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    endpoint = models.TextField()  # The name of the endpoint this refers to
    raw_data = models.TextField()  # Store raw received data to be parsed by a separate parser
    parse_started = models.DateTimeField(null=True)
    parse_succeeded = models.DateTimeField(null=True)
    parse_failed = models.DateTimeField(null=True)
    parse_fail_info = models.TextField(null=True)  # To store stack traces or other info about the fail
