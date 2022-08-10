from django import forms
from .serializers import AreaSerializer, LineSerializer

from peoplemeasurement.models import Area, Line


class BaseForm(forms.ModelForm):
    def clean_coordinates(self) -> list:
        coordinates = self.cleaned_data.get('coordinates')
        if not coordinates:
            return []
        try:
            self.validate_coordinates(coordinates)
        except forms.ValidationError:
            raise
        except Exception:
            raise forms.ValidationError("The coordinates cannot be interpreted. Format the input like the example")
        return coordinates

    def validate_coordinates(self, coordinates: list):
        raise NotImplementedError("Subclass this class and implement this method")

    def clean(self):
        cleaned_data = super().clean()
        coordinates = cleaned_data.get('coordinates')
        if coordinates:
            json_input = self.format_json_input(cleaned_data)
            serializer_instance = self.serializer(data=json_input)
            if not serializer_instance.is_valid():
                raise forms.ValidationError(' | '.join(serializer_instance.get_validation_errors()))
            cleaned_data["serializer_instance"] = serializer_instance
        elif not cleaned_data.get("geom"):
            raise forms.ValidationError("No coordinates defined")
        return cleaned_data

    def format_json_input(self, cleaned_data: dict):
        raise NotImplementedError("Subclass this class and implement this method")

    def save(self, commit=True):
        instance = super().save(commit=commit)
        coordinates = self.cleaned_data.get('coordinates')
        if coordinates:
            # There is json input for coordinates, so we overwrite all fields with the info from the json
            serializer_instance = self.cleaned_data["serializer_instance"]
            serializer_instance.update(instance=instance, validated_data=serializer_instance.validated_data)

        return instance


class LineForm(BaseForm):
    serializer = LineSerializer
    coordinates = forms.JSONField(required=False, help_text="""<b>Example:</b> <pre>    [
        [52.3,4.8],
        [52.3,4.9]
    ]</pre>""")

    def format_json_input(self, cleaned_data: dict) -> dict:
        json_input = {
            "name": cleaned_data["name"],
            "sensor": cleaned_data["sensor"],
            "azimuth": cleaned_data["azimuth"],
            "geom": {
                "type": "LineString",
                "coordinates": cleaned_data["coordinates"]
            }
        }
        return json_input

    def validate_coordinates(self, coordinates: list):
        if len(coordinates) < 2:
            raise forms.ValidationError("At least 2 points need to be defined")

    class Meta:
        model = Line
        fields = '__all__'


class AreaForm(BaseForm):
    serializer = AreaSerializer
    coordinates = forms.JSONField(required=False, help_text="""<b>Example:</b> <pre>    [
        [52.3,4.8],
        [52.3,4.9],
        [52.4,4.9],
        [52.4,4.8],
        [52.3,4.8]
    ]</pre>""")

    def format_json_input(self, cleaned_data: dict) -> dict:
        json_input = {
            "name": cleaned_data["name"],
            "sensor": cleaned_data["sensor"],
            "area": cleaned_data["area"],
            "geom": {
                "type": "Polygon",
                "coordinates": [cleaned_data["coordinates"]]
            }
        }
        return json_input

    def validate_coordinates(self, coordinates: list):
        if coordinates[0] != coordinates[-1]:
            raise forms.ValidationError("The start and end coordinate need to be identical")
        if len(coordinates) < 4:
            raise forms.ValidationError("At least 4 points need to be defined to form an area")

    class Meta:
        model = Area
        fields = '__all__'
