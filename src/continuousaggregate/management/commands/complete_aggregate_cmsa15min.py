import logging
from datetime import date, datetime

from django.core.management.base import BaseCommand
from django.db import connection, transaction

from continuousaggregate.models import Cmsa15Min

log = logging.getLogger(__name__)


@transaction.atomic
class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('--recalculate-history', default=False)
        parser.add_argument('--recalculate-history-since', type=date.fromisoformat)

    def handle(self, *args, **options):
        # calculate run_id based on the latest run_id from execution_log table
        with connection.cursor() as cursor:
            self.stdout.write(f"Start calculate next value for run_id based on execution_log table ")
            start = datetime.now()
        
            cursor.execute('select coalesce(max(run_id::int) +1, 1) from log.execution_log')
            run_id = cursor.fetchone()[0]
            
            finished_message = f"Finished run_id calculation in" \
                               f" {(datetime.now() - start).seconds} seconds"
            self.stdout.write(finished_message)
        
        # target_table := 'continuousaggregate_cmsa15min',
        complete_query = \
        f"""call prc.proc_pre_post_process (
                source_schema := 'public',
                source_table := 'vw_cmsa_15min_v01_aggregate',
                process_schema := 'prc',
                target_schema := 'public',
                target_table := '{Cmsa15Min._meta.db_table}',
                process_type := 'IU',
                implicit_deletes := false,
                run_id := {run_id},
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
