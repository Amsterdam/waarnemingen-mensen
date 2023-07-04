from random import randint

import pytest
from django.db.models import F, Q
from model_bakery import baker
from model_bakery.recipe import Recipe

from telcameras_v2.models import CountAggregate, Observation
from telcameras_v2.tools import scramble_count_aggregate
from tests.tools_for_testing import call_man_command


@pytest.mark.django_db
class TestTools:
    def test_scramble_counts_vanilla(self):
        count_agg = baker.make(CountAggregate)
        count_agg.count_in = 1
        count_agg.count_out = 1
        count_agg.count = 1
        count_agg.count_in_scrambled = None
        count_agg.count_out_scrambled = None
        count_agg.count_scrambled = None

        count_agg = scramble_count_aggregate(count_agg)

        assert count_agg.count_in_scrambled in (0, 1, 2)
        assert count_agg.count_out_scrambled in (0, 1, 2)
        assert count_agg.count_scrambled in (0, 1, 2)

    def test_scramble_counts_with_counts_none(self):
        count_agg = baker.make(CountAggregate)
        count_agg.count_in = None
        count_agg.count_out = None
        count_agg.count = None
        count_agg.count_in_scrambled = None
        count_agg.count_out_scrambled = None
        count_agg.count_scrambled = None

        count_agg = scramble_count_aggregate(count_agg)

        assert count_agg.count_in_scrambled is None
        assert count_agg.count_out_scrambled is None
        assert count_agg.count_scrambled is None

    def test_scramble_counts_doesnt_overwrite(self):
        count_agg = baker.make(CountAggregate)
        count_agg.count_in = 1
        count_agg.count_out = 1
        count_agg.count = 1
        count_agg.count_in_scrambled = 1
        count_agg.count_out_scrambled = 1
        count_agg.count_scrambled = 1

        count_agg = scramble_count_aggregate(count_agg)

        assert count_agg.count_in_scrambled == 1
        assert count_agg.count_out_scrambled == 1
        assert count_agg.count_scrambled == 1

    def test_scramble_counts_with_counts_zero(self):
        count_agg = baker.make(CountAggregate)
        count_agg.count_in = 0
        count_agg.count_out = 0
        count_agg.count = 0
        count_agg.count_in_scrambled = None
        count_agg.count_out_scrambled = None
        count_agg.count_scrambled = None

        count_agg = scramble_count_aggregate(count_agg)

        assert count_agg.count_in_scrambled in (0, 1)
        assert count_agg.count_out_scrambled in (0, 1)
        assert count_agg.count_scrambled in (0, 1)

    def test_scramble_v2_counts_command(self):
        count_aggregate_recipe = Recipe(
            CountAggregate,
            count_in=randint(0, 1000),
            count_out=randint(0, 1000),
            count=randint(0, 1000),
            count_in_scrambled=None,
            count_out_scrambled=None,
            count_scrambled=None,
        )
        count_aggregate_recipe.make(_quantity=100)

        # Do the scrambling
        call_man_command("scramble_v2_counts")

        differ_count_in = 0
        differ_count_out = 0
        differ_count = 0
        for ca in CountAggregate.objects.all():
            assert ca.count_in_scrambled is not None
            assert ca.count_in_scrambled in (
                ca.count_in - 1,
                ca.count_in,
                ca.count_in + 1,
            )
            if ca.count_in_scrambled != ca.count_in:
                differ_count_in += 1

            assert ca.count_out_scrambled is not None
            assert ca.count_out_scrambled in (
                ca.count_out - 1,
                ca.count_out,
                ca.count_out + 1,
            )
            if ca.count_out_scrambled != ca.count_out:
                differ_count_out += 1

            assert ca.count_scrambled is not None
            assert ca.count_scrambled in (ca.count - 1, ca.count, ca.count + 1)
            if ca.count_scrambled != ca.count:
                differ_count += 1

        # check all records have their scrambled counts set
        assert not CountAggregate.objects.filter(
            Q(count_in_scrambled=None)
            | Q(count_out_scrambled=None)
            | Q(count_scrambled=None)
        ).exists()

        # check that all scrambled counts are within valid range
        assert not CountAggregate.objects.filter(
            Q(count_in_scrambled__gt=F("count_in") + 1)
            | Q(count_in_scrambled__lt=F("count_in") - 1),
            Q(count_out_scrambled__gt=F("count_out") + 1)
            | Q(count_out_scrambled__lt=F("count_out") - 1),
            Q(count_scrambled__gt=F("count") + 1)
            | Q(count_scrambled__lt=F("count") - 1),
        )

        # Make sure that a significant amount of counts_scrambled were actually changed from the original
        assert differ_count_in > 50
        assert differ_count_out > 50
        assert differ_count > 50
