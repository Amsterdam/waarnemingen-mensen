from io import StringIO

from django.core.management import call_command


def call_man_command(command, *args, **kwargs):
    out = StringIO()
    call_command(
        command,
        *args,
        stdout=out,
        stderr=StringIO(),
        **kwargs,
    )
    return out.getvalue()
