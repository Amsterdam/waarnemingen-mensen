from django import forms
from django.contrib import admin
from django.contrib.gis.db import models as geomodels
from django.contrib.gis.geos import Point

from peoplemeasurement.models import Sensors


class LatLongWidget(forms.MultiWidget):
    """
    A Widget that splits Point input into latitude/longitude text inputs.
    """

    def __init__(self, attrs=None, date_format=None, time_format=None):
        widgets = (forms.TextInput(attrs=attrs),
                   forms.TextInput(attrs=attrs))
        super(LatLongWidget, self).__init__(widgets, attrs)

    def decompress(self, value):
        if value:
            return tuple(value.coords)
        return (None, None)

    def value_from_datadict(self, data, files, name):
        mylat = data[name + '_0']
        mylong = data[name + '_1']

        try:
            point = Point(float(mylat), float(mylong))
        except ValueError:
            return ''

        return point


@admin.register(Sensors)
class SensorAdmin(admin.ModelAdmin):
    list_display = ['id', 'objectnummer', 'soort']
    formfield_overrides = {geomodels.PointField: {'widget': LatLongWidget}}
