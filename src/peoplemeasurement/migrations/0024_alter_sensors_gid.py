# Generated by Django 3.2.4 on 2021-08-15 14:58

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("peoplemeasurement", "0023_auto_20210707_1224"),
    ]

    operations = [
        migrations.AlterField(
            model_name="sensors",
            name="gid",
            field=models.IntegerField(unique=True),
        ),
    ]
