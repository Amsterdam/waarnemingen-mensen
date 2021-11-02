from django import forms
from django.contrib import admin
from django.contrib.gis.db import models as geomodels
from django.contrib.gis.geos import Point
from django.forms.widgets import TextInput
from import_export.admin import ImportExportModelAdmin
from import_export.fields import Field
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
    imported_rows_pks = []  # save pk's of imported rows to delete the rest

    class Meta:
        model = Sensors
        exclude = ['id']
        import_id_fields = ['gid']
        # fields = ['gid', 'objectnummer', 'soort']
        # export_order = ('id', 'price', 'author', 'name')
        # skip_unchanged = True

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
            self.imported_rows_pks.append(row_result.object_id)

    def after_import(self, dataset, result, using_transactions, dry_run, **kwargs):
        # Remove all rows which were absent in the import
        Sensors.objects.exclude(pk__in=set(self.imported_rows_pks)).delete()
        ModelResource.after_import(self, dataset, result, using_transactions, dry_run, **kwargs)

# class AreaInline(admin.StackedInline):
#     model = Area

@admin.register(Sensors)
class SensorAdmin(ImportExportModelAdmin, admin.ModelAdmin):
    list_display = ['id', 'gid', 'objectnummer', 'soort']
    formfield_overrides = {geomodels.PointField: {'widget': LatLongWidget}}
    tmp_storage_class = CacheStorage
    resource_class = SensorResource
    # inlines = [AreaInline,]


@admin.register(Servicelevel)
class ServicelevelAdmin(ImportExportModelAdmin, admin.ModelAdmin):
    list_display = ['type_parameter', 'type_gebied', 'type_tijd', 'level_nr', 'level_label', 'lowerlimit', 'upperlimit']
    tmp_storage_class = CacheStorage


@admin.register(Area)
class AreaAdmin(LeafletGeoAdminMixin, admin.ModelAdmin):
    list_display = ['sensor', 'name', 'geom', 'area']
    tmp_storage_class = CacheStorage
    # formfield_overrides = {geomodels.PolygonField: {'widget': TextInput}}


@admin.register(Line)
class LineAdmin(LeafletGeoAdminMixin, admin.ModelAdmin):
    list_display = ['sensor', 'name', 'geom', 'azimuth']
    tmp_storage_class = CacheStorage
    # formfield_overrides = {geomodels.PolygonField: {'widget': TextInput}}
