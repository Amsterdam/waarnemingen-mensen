# Generated by Django 3.0.7 on 2020-07-15 13:25

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('telcameras_v2', '0003_countaggregate_observation_personaggregate'),
    ]

    operations = [
        migrations.AddField(
            model_name='countaggregate',
            name='area',
            field=models.FloatField(null=True),
        ),
        migrations.AddField(
            model_name='countaggregate',
            name='count',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='countaggregate',
            name='geom',
            field=models.TextField(null=True),
        ),
        migrations.AlterField(
            model_name='countaggregate',
            name='azimuth',
            field=models.IntegerField(null=True),
        ),
        migrations.AlterField(
            model_name='countaggregate',
            name='count_in',
            field=models.IntegerField(null=True),
        ),
        migrations.AlterField(
            model_name='countaggregate',
            name='count_out',
            field=models.IntegerField(null=True),
        ),
    ]