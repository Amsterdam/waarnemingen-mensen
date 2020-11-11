from django.apps import AppConfig


class TimescaleConfig(AppConfig):
    name = "contrib.timescale"
    verbose_name = "Timescale"

    def ready(self):
        pass
