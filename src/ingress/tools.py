import re

from ingress.models import Endpoint


def add_endpoint(url_key):
    if len(url_key) > 255:
        return None, "The url_key is larger than 255 characters. Please choose a shorter url_key."

    # Check that it only consists of sane characters
    if not re.match("^[A-Za-z0-9_-]*$", url_key):
        return None, "The url_key can only contain numbers, letters, underscores and dashes."

    # Check whether it already exists
    if Endpoint.objects.filter(url_key=url_key).count() > 0:
        return None, f"The endpoint '{url_key}' already exists"

    # Create the endpoint and return it
    endpoint_obj = Endpoint.objects.create(url_key=url_key)
    if endpoint_obj.id:
        return endpoint_obj, f"Created endpoint with url_key '{url_key}'"

    return None, f"FAILED to create the endpoint with url_key '{url_key}'"
