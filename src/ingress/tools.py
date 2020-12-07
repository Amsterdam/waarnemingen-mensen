import re

from django.db import transaction

from ingress.models import Endpoint, FailedIngressQueue, IngressQueue


class NewEndpointError(Exception):
    pass


class RedoFailedMessagesError(Exception):
    pass


def add_endpoint(url_key):
    if len(url_key) > 255:
        raise NewEndpointError("The url_key is larger than 255 characters. Please choose a shorter url_key.")

    # Check that it only consists of sane characters
    if not re.match("^[A-Za-z0-9_-]*$", url_key):
        raise NewEndpointError("The url_key can only contain numbers, letters, underscores and dashes.")

    # Check whether it already exists
    if Endpoint.objects.filter(url_key=url_key).count() > 0:
        raise NewEndpointError(f"The endpoint '{url_key}' already exists")

    # Create the endpoint and return it
    endpoint_obj = Endpoint.objects.create(url_key=url_key)
    if endpoint_obj.id:
        return endpoint_obj

    raise NewEndpointError(f"FAILED to create the endpoint with url_key '{url_key}'")


def redo_failed_ingress_messages(url_key):
    try:
        endpoint = Endpoint.objects.get(url_key=url_key)
    except Endpoint.DoesNotExist:
        raise RedoFailedMessagesError(f"\n\nThe endpoint with url_key '{url_key}' does not exist. Nothing has been done.\n\n")

    moved_counter = 0
    with transaction.atomic():
        failed_ingresses = FailedIngressQueue.objects.filter(endpoint=endpoint).select_for_update(skip_locked=True)
        if failed_ingresses.count() == 0:
            raise RedoFailedMessagesError("No messages were found, so nothing was done.")

        for failed_ingress in failed_ingresses:
            ingress = IngressQueue()
            ingress.created_at = failed_ingress.created_at
            ingress.endpoint = failed_ingress.endpoint
            ingress.raw_data = failed_ingress.raw_data
            ingress.save()
            failed_ingress.delete()
            moved_counter += 1

    return moved_counter
