from datetime import timedelta

import pytest
from django.utils.timezone import now
from model_bakery import baker

import time_machine
from telcameras_v3.models import Observation
from tests.tools_for_testing import call_man_command


@pytest.mark.django_db
class TestCommands:
    def test_monitor_incoming_messages(self):
        # There is no message yet, so we expect an error
        with pytest.raises(AssertionError):
            call_man_command('monitor_v3_incoming_messages')

        # Insert one observation
        baker.make(Observation)

        # There is one recent observation so we don't expect an error now
        call_man_command('monitor_v3_incoming_messages')

        # We travel 20 minutes in the future, and there we do expect an error
        with time_machine.travel(now()+timedelta(minutes=20)):
            with pytest.raises(AssertionError):
                call_man_command('monitor_v3_incoming_messages')
