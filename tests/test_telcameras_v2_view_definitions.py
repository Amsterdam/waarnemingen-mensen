import pytest
from src.telcameras_v2.view_definitions import VIEW_STRINGS, get_view_strings

MOCK_VIEW_STRINGS = {
    'test_view_v100': "CREATE VIEW test_view_v100 AS SELECT * FROM table;"
}


@pytest.mark.django_db
class TestViewDefinitions:
    def test_get_view_strings(self):
        mock_view_name = 'test_view_v100'
        indexes = [('column1', 'column2'), ('column3',)]

        view_strings = get_view_strings(MOCK_VIEW_STRINGS, mock_view_name, indexes=indexes)

        assert view_strings['sql'] == MOCK_VIEW_STRINGS[mock_view_name]
        assert view_strings['reverse_sql'] == f"DROP VIEW IF EXISTS {mock_view_name};"
        assert view_strings['sql_materialized'] == f"""
        CREATE MATERIALIZED VIEW {mock_view_name}_materialized AS
        SELECT * FROM {mock_view_name};
        """
        assert view_strings['reverse_sql_materialized'] == \
               f"DROP MATERIALIZED VIEW IF EXISTS {mock_view_name}_materialized;"
        assert len(view_strings['indexes']) == 2
        assert view_strings['indexes'][0] == f"""
                CREATE UNIQUE INDEX {mock_view_name}_materialized_column1_column2_idx 
                ON public.{mock_view_name}_materialized USING btree (column1, column2);
                """
        assert view_strings['indexes'][1] == f"""
                CREATE UNIQUE INDEX {mock_view_name}_materialized_column3_idx 
                ON public.{mock_view_name}_materialized USING btree (column3);
                """
