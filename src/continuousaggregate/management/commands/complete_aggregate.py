import logging
from datetime import datetime

from django.core.management.base import BaseCommand
from django.db import connection, transaction

from continuousaggregate.models import Cmsa15Min

log = logging.getLogger(__name__)


@transaction.atomic
class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument('table_name', type=str)
        parser.add_argument('--recalculate-history', default=False)

    def handle(self, *args, **options):
        table_name = options['table_name']
        recalculate_history = options['recalculate_history']

        if recalculate_history:
            self.stdout.write(f"Start deleting full aggregation table {Cmsa15Min._meta.db_table}")
            Cmsa15Min.objects.all().delete()
            self.stdout.write(f"Finished deleting full aggregation table {Cmsa15Min._meta.db_table}")

        # target_table := 'continuousaggregate_cmsa15min',
        complete_query = \
        f"""call prc.proc_pre_post_process (
                source_schema := 'public',
                source_table := 'vw_cmsa_15min_v01_aggregate',
                process_schema := 'prc',
                target_schema := 'public',
                target_table := '{table_name}',
                process_type := 'IU',
                implicit_deletes := false,
                run_id := null,
                parent_component := '' ,
                ultimate_parent_component := '',
                logfromlevel := 2,
                rebuild_spatial_index := false
            );
        """
        with connection.cursor() as cursor:
            self.stdout.write(f"Start completing aggregation table {Cmsa15Min._meta.db_table}")
            start = datetime.now()
            cursor.execute(complete_query)
            finished_message = f"Finished completing aggregation table {Cmsa15Min._meta.db_table} in" \
                               f" {(datetime.now() - start).seconds} seconds"
            self.stdout.write(finished_message)
