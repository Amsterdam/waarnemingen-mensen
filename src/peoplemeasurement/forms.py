from django import forms
from django.contrib.gis.geos import LineString, Polygon

from peoplemeasurement.models import Area, Sensors


class AreaForm(forms.ModelForm):
    json_input = forms.JSONField(required=False)

    def clean(self):
        cleaned_data = super().clean()

        # TODO: Use a serializer to do this properly
        # TODO: Also raise a ValidationError if it's a new object and no json is inserted (this is not possible, since the area points can only be inserted using the json).
        json_input = cleaned_data.get('json_input')
        if json_input:
            # Check keys
            for k in ('sensor', 'areas'):
                if not json_input.get(k):
                    raise forms.ValidationError(f"{k} missing in json")
            for k in ('area_id', 'area', 'points'):
                if not json_input['areas'].get('area_id'):
                    raise forms.ValidationError(f"{k} missing in json")

            # Check if sensor exists
            # TODO: move this to the model
            if Sensors.objects.filter(objectnummer=json_input['sensor']).count() == 0:
                raise forms.ValidationError(f"Sensor with objectnummer '{json_input['sensor']}' does not exist.")

            # Check points
            points = json_input['areas']['points']
            if len(points) <= 3:  # The last point should be the same as the first point, and we need at least a triangle to have an area
                raise forms.ValidationError("Not enough points")
            if points[0] != points[-1]:
                raise forms.ValidationError("The geom points in the json do not form a closed loop.")

        return cleaned_data

    def save(self, commit=True):
        instance = super().save(commit=commit)
        json_input = self.cleaned_data.get('json_input', None)
        if json_input and json_input != 'null':
            # There is json input, so we overwrite all fields with the info from the json
            sensor_objectnummer = json_input['sensor']
            instance.sensor = Sensors.objects.filter(objectnummer=sensor_objectnummer)[0]
            instance.name = json_input['areas']['area_id']
            instance.area = json_input['areas']['area']
            geom_points = json_input['areas']['points']
            instance.geom = Polygon([(coordinate['longitude'], coordinate['latitude']) for coordinate in geom_points])
            if commit:
                instance.save()

        return instance

    class Meta:
        model = Area
        fields = '__all__'


class LineForm(forms.ModelForm):
    json_input = forms.JSONField(required=False)

    def clean(self):
        cleaned_data = super().clean()

        # TODO: Use a serializer to do this properly
        # TODO: Also raise a ValidationError if it's a new object and no json is inserted (this is not possible, since the area points can only be inserted using the json).
        json_input = cleaned_data.get('json_input')
        if json_input:
            # Check keys
            for k in ('sensor', 'lines'):
                if not json_input.get(k):
                    raise forms.ValidationError(f"{k} missing in json")
            for k in ('line_id', 'azimuth', 'points'):
                if not json_input['lines'].get(k):
                    raise forms.ValidationError(f"{k} missing in json")

            # Check if sensor exists
            # TODO: move this to the model
            if Sensors.objects.filter(objectnummer=json_input['sensor']).count() == 0:
                raise forms.ValidationError(f"Sensor with objectnummer '{json_input['sensor']}' does not exist.")

            # Check points
            points = json_input['lines']['points']
            if len(points) < 2:
                raise forms.ValidationError("We need at least two points")

        return cleaned_data

    def save(self, commit=True):
        instance = super().save(commit=commit)
        json_input = self.cleaned_data.get('json_input', None)
        if json_input:
            # There is json input, so we overwrite all fields with the info from the json
            sensor_objectnummer = json_input['sensor']
            instance.sensor = Sensors.objects.filter(objectnummer=sensor_objectnummer)[0]
            instance.name = json_input['lines']['line_id']
            instance.azimuth = json_input['lines']['azimuth']
            geom_points = json_input['lines']['points']
            instance.geom = LineString([(coordinate['longitude'], coordinate['latitude']) for coordinate in geom_points])
            if commit:
                instance.save()

        return instance

    class Meta:
        model = Area
        fields = '__all__'
