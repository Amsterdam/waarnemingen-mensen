from django import forms
from .serializers import AreaSerializer, LineSerializer

from peoplemeasurement.models import Area, Line


class BaseForm(forms.ModelForm):
    geom_type = "Undefined"
    required_fields = []

    def clean(self):
        cleaned_data = super().clean()
        json_input = cleaned_data.get('json_input')
        if json_input:
            serializer_instance = self.serializer(data=json_input)
            if not serializer_instance.is_valid():
                raise forms.ValidationError(' | '.join(serializer_instance.get_validation_errors()))
            if not json_input["geom"]["type"] == self.geom_type:
                raise forms.ValidationError(f"The type of geom should be {self.geom_type}")
            cleaned_data["serializer_instance"] = serializer_instance
        else:
            for field in self.required_fields:
                if not cleaned_data.get(field):
                    raise forms.ValidationError(f"The field {field} cannot be empty when no JSON is supplied")
        return cleaned_data

    def save(self, commit=True):
        instance = super().save(commit=commit)
        json_input = self.cleaned_data.get('json_input')
        if json_input:
            # There is json input, so we overwrite all fields with the info from the json
            serializer_instance = self.cleaned_data["serializer_instance"]
            serializer_instance.update(instance=instance, validated_data=serializer_instance.validated_data)

        return instance

class LineForm(BaseForm):
    serializer = LineSerializer
    required_fields = ['name', 'sensor', 'azimuth', 'geom']
    geom_type = "LineString"
    json_input = forms.JSONField(required=False, help_text="""<b>Example:</b> <pre>{
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
    }</pre>""")

    class Meta:
        model = Line
        fields = '__all__'


class AreaForm(BaseForm):
    serializer = AreaSerializer
    required_fields = ['name', 'sensor', 'area', 'geom']
    geom_type = "Polygon"
    json_input = forms.JSONField(required=False, help_text="""<b>Example:</b> <pre>{
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
    }</pre>""")

    class Meta:
        model = Area
        fields = '__all__'
