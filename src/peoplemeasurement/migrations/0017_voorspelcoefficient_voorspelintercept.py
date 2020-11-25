# Generated by Django 2.2.16 on 2020-11-17 16:22

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('peoplemeasurement', '0016_sensors_is_active'),
    ]

    operations = [
        migrations.CreateModel(
            name='VoorspelCoefficient',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('sensor', models.CharField(max_length=255)),
                ('bron_kwartier_volgnummer', models.IntegerField()),
                ('toepassings_kwartier_volgnummer', models.IntegerField()),
                ('coefficient_waarde', models.FloatField()),
            ],
        ),
        migrations.CreateModel(
            name='VoorspelIntercept',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('sensor', models.CharField(max_length=255)),
                ('toepassings_kwartier_volgnummer', models.IntegerField()),
                ('intercept_waarde', models.FloatField()),
            ],
        ),
    ]