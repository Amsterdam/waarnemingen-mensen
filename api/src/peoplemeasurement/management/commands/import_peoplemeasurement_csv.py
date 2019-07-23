import logging

import io

from objectstore import objectstore

from django.core.management.base import BaseCommand
from django.db import connection
from django.conf import settings

from peoplemeasurement.models import PeopleMeasurementCSV, PeopleMeasurementCSVTemp
from peoplemeasurement.objectstore_util import get_objstore_file, get_objstore_directory_meta


logging.basicConfig(level=logging.DEBUG, format='%(message)s')
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

OBJECTSTORE_DIRECTORY = 'cs'
OBJECTSTORE_SUBDIRECTORY = 'full_week'


def copy_csv(csv, table_name, delimiter):
    '''
    Executes copy_expert to import a csv file to postgres through sql.
    Significantly faster than using django ORM.

    Parameters:
    csv (io-stream): The csv file to be imported
    table_name (str): The name of the table the csv will be imported into
    delimiter (str): The csv delimiter
    '''

    with connection.cursor() as cursor:
        cursor.copy_expert(f"""
        COPY {table_name} FROM STDIN WITH (FORMAT CSV, DELIMITER '{delimiter}')
        """, csv)


class Command(BaseCommand):
    '''
    Imports Crowd monitoring csv files from object store.

    The process works as follows:
        1- Object store directory is retrieved
        2- Get list of not imported csv files
        3- Delete temporary table
        4- Import file into temporary table
        5- Insert into regular table with csv name appended
        6- Repeat from step 3 till all files are imported
    '''

    def add_arguments(self, parser):
        pass


    def get_csv_list(self, objstore_csvs):
        '''
        Compares the list of csv files found on the object store
        with the imported csv files and returns the difference.

        The file needs to follow these rules:
            - Is a .csv file
            - Falls under the correct subdirectory
            - Is not already imported in the db

        '''
        csv_list = []
        # Get a list of imported csv files.
        csv_names = (
            PeopleMeasurementCSV.objects.values_list('csv_name', flat=True)
            .order_by('csv_name')
            .distinct('csv_name')
        )

        # Go through the object store csvs
        for file_meta in objstore_csvs:
            # Check if file follows the necessary rules
            file_rules_ok = all([
                file_meta['name'].startswith(OBJECTSTORE_SUBDIRECTORY + '/'),
                file_meta['name'].endswith('.csv'),  # correct extensions
                file_meta['name'] not in csv_names  # hasn't been imported yet
            ])

            if file_rules_ok:
                # Append the list with the files not yet imported
                csv_list.append(file_meta)
        return csv_list


    def insert_from_temp(self, file_name):
        '''
        Inserts all records from temporary table into the normal
        table. The columns are dynamically retrieved from the models.
        The file_name param is the csv file name that is appended to the
        regular model.
        '''
        columns = [
            f.name for f in PeopleMeasurementCSV._meta.get_fields()
        ]
        temp_columns = [
            f.name for f in PeopleMeasurementCSVTemp._meta.get_fields()
        ]

        # prepare for insertinto fields in query
        columns = ", ".join(columns)
        # prepare for select fields in query
        temp_columns = ", ".join(temp_columns)

        with connection.cursor() as cursor:
            cursor.execute(f"""
            INSERT INTO {PeopleMeasurementCSV._meta.db_table} (
                {columns}
            )
            SELECT
                {temp_columns},
                '{file_name}'
            FROM
                {PeopleMeasurementCSVTemp._meta.db_table};
            """)

    def handle(self, *args, **options):
        log.info("Starting Import")

        directory_meta = get_objstore_directory_meta(OBJECTSTORE_DIRECTORY)
        csv_list = self.get_csv_list(directory_meta)
        log.info(f'Importing {len(csv_list)} file(s)')

        for csv_meta in csv_list:
            temp_name = PeopleMeasurementCSVTemp._meta.db_table
            file_name = csv_meta['name']

            log.info('Truncating temporary table')
            PeopleMeasurementCSVTemp.objects.all().delete()

            log.info(f'Downloading {file_name}')
            csv_file = get_objstore_file(file_name, OBJECTSTORE_DIRECTORY)[1]
            stream = io.BytesIO(csv_file)
            log.info(f'Importing {file_name}')
            copy_csv(stream, temp_name, ';')
            self.insert_from_temp(file_name)
            log.info('Done')
