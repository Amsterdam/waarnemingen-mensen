from django import forms
from django.contrib import admin
from django.contrib.gis.db import models as geomodels
from django.contrib.gis.geos import Point

from import_export.admin import ImportExportModelAdmin
from import_export.tmp_storages import CacheStorage
from peoplemeasurement.models import (Area, Line, Sensors, Servicelevel,
                                      VoorspelCoefficient, VoorspelIntercept)


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
class SensorAdmin(ImportExportModelAdmin, admin.ModelAdmin):
    list_display = ['id', 'gid', 'objectnummer', 'soort']
    formfield_overrides = {geomodels.PointField: {'widget': LatLongWidget}}
    tmp_storage_class = CacheStorage


@admin.register(Servicelevel)
class ServicelevelAdmin(ImportExportModelAdmin, admin.ModelAdmin):
    list_display = ['type_parameter', 'type_gebied', 'type_tijd', 'level_nr', 'level_label', 'lowerlimit', 'upperlimit']
    tmp_storage_class = CacheStorage


@admin.register(VoorspelCoefficient)
class VoorspelCoefficientAdmin(ImportExportModelAdmin, admin.ModelAdmin):
    list_display = ['sensor', 'bron_kwartier_volgnummer', 'toepassings_kwartier_volgnummer', 'coefficient_waarde']
    tmp_storage_class = CacheStorage


@admin.register(VoorspelIntercept)
class VoorspelInterceptAdmin(ImportExportModelAdmin, admin.ModelAdmin):
    list_display = ['sensor', 'toepassings_kwartier_volgnummer', 'intercept_waarde']
    tmp_storage_class = CacheStorage


@admin.register(Area)
class AreaAdmin(ImportExportModelAdmin, admin.ModelAdmin):
    list_display = ['sensor', 'name', 'geom', 'area']
    tmp_storage_class = CacheStorage


@admin.register(Line)
class LineAdmin(ImportExportModelAdmin, admin.ModelAdmin):
    list_display = ['sensor', 'name', 'geom', 'azimuth']
    tmp_storage_class = CacheStorage
