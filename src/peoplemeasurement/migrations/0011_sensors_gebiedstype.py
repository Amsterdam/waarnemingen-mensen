# Generated by Django 3.0.9 on 2020-08-17 13:32

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("peoplemeasurement", "0010_cmsa_1h_count_view_v1"),
    ]

    operations = [
        migrations.AddField(
            model_name="sensors",
            name="gebiedstype",
            field=models.CharField(max_length=255, null=True),
        ),
    ]
