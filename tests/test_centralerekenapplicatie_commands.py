from datetime import timedelta

import pytest
from django.utils.timezone import now
from model_bakery import baker

import time_machine
from centralerekenapplicatie_v1.models import AreaMetric, LineMetric
from tests.tools_for_testing import call_man_command


baker.generators.add('contrib.timescale.fields.TimescaleDateTimeField', now)


@pytest.mark.django_db
class TestCommands:

    @pytest.mark.parametrize(
        'metric', [AreaMetric, LineMetric]
    )
    def test_monitor_incoming_messages(self, metric):
        # There is no message yet, so we expect an error
        with pytest.raises(AssertionError):
            call_man_command('monitor_cra_incoming_messages')

        # Insert one metric
        baker.make(metric)

        # There is only one record of the expected two types so we expect an error now
        call_man_command('monitor_cra_incoming_messages')

        # We travel 20 minutes in the future, and there we do expect an error
        with time_machine.travel(now()+timedelta(minutes=20)):
            with pytest.raises(AssertionError):
                call_man_command('monitor_cra_incoming_messages')
