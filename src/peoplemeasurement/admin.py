from django.contrib.gis.geos.error import GEOSException
from django import forms
from django.contrib import admin
from django.contrib.gis.db import models as geomodels
from django.contrib.gis.geos import Point, Polygon
from import_export.admin import ImportExportModelAdmin
from import_export.resources import ModelResource
from import_export.tmp_storages import CacheStorage
from leaflet.admin import LeafletGeoAdminMixin

from peoplemeasurement.models import Area, Line, Sensors, Servicelevel


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


class SensorResource(ModelResource):
    # save PKs of imported rows to delete the rest
    imported_sensor_pks = []
    imported_area_pks = []
    imported_line_pks = []

    class Meta:
        model = Sensors
        exclude = ['id']
        import_id_fields = ['gid']

    def after_export(self, queryset, data, *args, **kwargs):
        areas = []
        lines = []
        for sensor in queryset:
            area_dicts = []
            for area in sensor.areas.all():
                area_dicts.append({
                    'name': area.name,
                    'geom': area.geom.__str__(),
                    'area': area.area
                })

            line_dicts = []
            for line in sensor.lines.all():
                line_dicts.append({
                    'name': line.name,
                    'geom': line.geom.__str__(),
                    'azimuth': line.azimuth
                })
            areas.append(area_dicts)
            lines.append(line_dicts)

        if areas:
            data.append_col(areas, header='areas')
        if lines:
            data.append_col(lines, header='lines')

    def after_import_row(self, row, row_result, row_number=None, **kwargs):
        # Save the pk's from the instances that were touched so that we can remove the rest later
        if row_result.object_id:
            self.imported_sensor_pks.append(row_result.object_id)

        if row['areas']:
            for area_dict in row['areas']:
                area_obj, created = Area.objects.get_or_create(
                    sensor_id=row_result.object_id, **area_dict)
                self.imported_area_pks.append(area_obj.id)

        if row['lines']:
            for line_dict in row['lines']:
                line_obj, created = Line.objects.get_or_create(
                    sensor_id=row_result.object_id, **line_dict)
                self.imported_area_pks.append(line_obj.id)

    def after_import(self, dataset, result, using_transactions, dry_run, **kwargs):
        # Remove all sensors, areas and lines which were absent in the import
        Sensors.objects.exclude(pk__in=self.imported_sensor_pks).delete()
        Area.objects.exclude(pk__in=self.imported_area_pks).delete()
        Line.objects.exclude(pk__in=self.imported_line_pks).delete()

        super().after_import(dataset, result, using_transactions, dry_run, **kwargs)

    # TODO: support changing an gid in the import (makes collision because objectnummer is unique)


@admin.register(Sensors)
class SensorAdmin(ImportExportModelAdmin, admin.ModelAdmin):
    list_display = ['objectnummer', 'gid', 'soort']
    formfield_overrides = {geomodels.PointField: {'widget': LatLongWidget}}
    tmp_storage_class = CacheStorage
    resource_class = SensorResource


@admin.register(Servicelevel)
class ServicelevelAdmin(ImportExportModelAdmin, admin.ModelAdmin):
    list_display = ['type_parameter', 'type_gebied', 'type_tijd', 'level_nr', 'level_label', 'lowerlimit', 'upperlimit']
    tmp_storage_class = CacheStorage


class AreaForm(forms.ModelForm):
    geom_json_input = forms.JSONField()

    def save(self, commit=True):
        geom_json_input = self.cleaned_data.get('geom_json_input', None)
        if geom_json_input:
            # There is json input, so we overwrite all fields with the info from the json
            try:
                instance = super(AreaForm, self).save(commit=commit)
                sensor_objectnummer = geom_json_input['sensor']
                instance.sensor = Sensors.objects.filter(objectnummer=sensor_objectnummer)[0]
                # TODO: catch error if sensor does not exist
                instance.name = geom_json_input['areas']['area_id']
                instance.area = geom_json_input['areas']['area']
                geom_points = geom_json_input['areas']['points']
                instance.geom = Polygon([(coordinate['longitude'], coordinate['latitude']) for coordinate in geom_points])
                # TODO: Catch error if points don't form a closed loop (if the last coordinates are not the same as the first one) django.contrib.gis.geos.error.GEOSException
                if commit:
                    instance.save()
            except GEOSException as e:
                return "a message that it's not a closed loop"
            except Exception as e:
                # TODO: log something here. Maybe give feedback in the UI?
                raise e

            return instance

    class Meta:
        model = Area
        fields = '__all__'


@admin.register(Area)
class AreaAdmin(LeafletGeoAdminMixin, admin.ModelAdmin):
    form = AreaForm
    fieldsets = (
        (None, {
            'fields': ('name', 'sensor', 'area', 'geom', 'geom_json_input'),
        }),
    )

    list_display = ['name', 'sensor', 'area']
    tmp_storage_class = CacheStorage

    # TODO:
    # - Maak json input niet required
    # - Haal "null" weg uit geom json input field
    # - Show leaflet ding zonder er dingen in op te kunnen slaan
    # - Give instructions in UI that json overwrites data in input fields



@admin.register(Line)
class LineAdmin(LeafletGeoAdminMixin, admin.ModelAdmin):
    list_display = ['name', 'sensor', 'geom', 'azimuth']
    tmp_storage_class = CacheStorage
