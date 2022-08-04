from django import forms
from .serializers import AreaSerializer, LineSerializer

from peoplemeasurement.models import Area, Line


class BaseForm(forms.ModelForm):
    def clean(self):
        cleaned_data = super().clean()
        json_input = cleaned_data.get('json_input')
        if json_input != 'null' and json_input:
            serializer_instance = self.serializer(data=json_input)
            if not serializer_instance.is_valid():
                raise forms.ValidationError(' | '.join(serializer_instance.get_validation_errors()))
            cleaned_data["serializer_instance"] = serializer_instance
        return cleaned_data

    def save(self, commit=True):
        instance = super().save(commit=commit)
        json_input = self.cleaned_data.get('json_input')
        if json_input and json_input != 'null':
            # There is json input, so we overwrite all fields with the info from the json
            serializer_instance = self.cleaned_data["serializer_instance"]
            serializer_instance.update(instance=instance, validated_data=serializer_instance.validated_data)

        return instance


class LineForm(BaseForm):
    serializer = LineSerializer
    json_input = forms.JSONField(required=False, help_text="""{
        "name": "test_name",
        "sensor": "sensor_123",
        "azimuth": 40,
        "geom": {
            "type": "LineString",
            "coordinates": [
                [52.3,4.8],
                [52.3,4.9]
            ]
        }
    }""")

    class Meta:
        model = Line
        fields = '__all__'


class AreaForm(BaseForm):
    serializer = AreaSerializer
    json_input = forms.JSONField(required=False, help_text="""{
        "name": "test_name",
        "sensor": "sensor_123",
        "area": 40,
        "geom": {
            "type": "Polygon",
            "coordinates": [[
                [52.3,4.8],
                [52.3,4.9],
                [52.4,4.9],
                [52.4,4.8],
                [52.3,4.8]
            ]]
        }
    }""")

    class Meta:
        model = Area
        fields = '__all__'
