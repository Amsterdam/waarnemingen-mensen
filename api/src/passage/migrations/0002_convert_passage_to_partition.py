from django.db import migrations, models
from datetime import datetime, timedelta

'''
postgresql v11 requires unique constraint columns to be part of the partition key.
We need to modify the primary key of passage_passage(id) to passage_passage(id, passage_at) and
use that table to create the new partition table. Don't forget to schedule make_partitions.py to create
the actual partitions!
'''

class Migration(migrations.Migration):

    initial = False

    dependencies = [
        ('passage', '0001_initial'),
    ]

    partition_date = datetime.today().date()
    partition_str = partition_date.strftime("%Y%m%d")
 
    operations = [
        migrations.RunSQL("""
            alter table passage_passage drop constraint passage_passage_pkey;
            alter table passage_passage add constraint passage_passage_pkey primary key (id, passage_at);
            alter table passage_passage rename to __old_passage;
            create table passage_passage (like __old_passage including all) partition by range (passage_at);
            drop table __old_passage;
            
            create table if not exists passage_passage_20181016 partition of passage_passage for values from ('2018-10-16') to ('2018-10-17');
        """),
        migrations.RunSQL(f"create table if not exists passage_passage_{partition_str} partition of passage_passage for values from ('{partition_date}') to ('{partition_date + timedelta(1)}');")
    ]
