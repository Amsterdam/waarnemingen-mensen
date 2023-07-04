from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("peoplemeasurement", "0004_cmsa_15min_view_20190926_1130"),
    ]

    _VIEW_NAME = "cmsa_15min_materialized"

    sql = f"""
CREATE MATERIALIZED VIEW {_VIEW_NAME} AS
SELECT * FROM cmsa_15min;
"""

    reverse_sql = f"DROP MATERIALIZED VIEW IF EXISTS {_VIEW_NAME};"

    operations = [
        migrations.RunSQL(sql=sql, reverse_sql=reverse_sql),
    ]
