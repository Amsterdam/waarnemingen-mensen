# Generated by Django 2.2.13 on 2020-07-16 08:45

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("telcameras_v2", "0004_auto_20200715_1525"),
    ]

    operations = [
        migrations.AlterField(
            model_name="countaggregate",
            name="azimuth",
            field=models.PositiveSmallIntegerField(null=True),
        ),
        migrations.AlterField(
            model_name="countaggregate",
            name="count",
            field=models.SmallIntegerField(null=True),
        ),
        migrations.AlterField(
            model_name="countaggregate",
            name="count_in",
            field=models.SmallIntegerField(null=True),
        ),
        migrations.AlterField(
            model_name="countaggregate",
            name="count_out",
            field=models.SmallIntegerField(null=True),
        ),
        migrations.AlterField(
            model_name="observation",
            name="interval",
            field=models.SmallIntegerField(),
        ),
    ]
