from django.db import models


class IngressQueue(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    endpoint = models.ForeignKey('Endpoint', on_delete=models.CASCADE)
    raw_data = models.TextField()  # Store raw received data to be parsed by a separate parser
    parse_started = models.DateTimeField(null=True)
    parse_succeeded = models.DateTimeField(null=True)
    parse_failed = models.DateTimeField(null=True)
    parse_fail_info = models.TextField(null=True)  # To store stack traces or other info about the fail


class Endpoint(models.Model):
    # The url_key is the end of the url and also the key with which it can be retrieved from the queue
    # For example, in the url /ingress/example, the string 'example' is the url_key
    url_key = models.CharField(max_length=255, unique=True)
