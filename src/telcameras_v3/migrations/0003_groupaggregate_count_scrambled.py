# Generated by Django 3.0.11 on 2021-01-20 13:24

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('telcameras_v3', '0002_timescale_20201214_1412'),
    ]

    operations = [
        migrations.AddField(
            model_name='groupaggregate',
            name='count_scrambled',
            field=models.SmallIntegerField(null=True),
        ),
    ]