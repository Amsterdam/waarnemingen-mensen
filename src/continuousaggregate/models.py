from django.db import models

from contrib.timescale.fields import TimescaleDateTimeField


class Cmsa15Min(models.Model):
    bk_continuousaggregate_cmsa15min    = models.CharField(primary_key=True, unique=True, max_length=255)
    sensor                              = models.CharField(max_length=255) 
    timestamp_rounded                   = TimescaleDateTimeField(interval="1 day")  # Timestamp of when the data was recorded in the camera
    total_count                         = models.IntegerField(null=True)
    count_down                          = models.IntegerField(null=True)
    count_up                            = models.IntegerField(null=True)
    density_avg                         = models.FloatField(null=True)
    basedonxmessages                    = models.IntegerField(null=True)
    total_count_p10                     = models.DecimalField(max_digits=16, decimal_places=13, null=True)
    total_count_p20                     = models.DecimalField(max_digits=16, decimal_places=13, null=True)
    total_count_p50                     = models.DecimalField(max_digits=16, decimal_places=13, null=True)
    total_count_p80                     = models.DecimalField(max_digits=16, decimal_places=13, null=True)
    total_count_p90                     = models.DecimalField(max_digits=16, decimal_places=13, null=True)
    count_down_p10                      = models.DecimalField(max_digits=16, decimal_places=13, null=True)
    count_down_p20                      = models.DecimalField(max_digits=16, decimal_places=13, null=True)
    count_down_p50                      = models.DecimalField(max_digits=16, decimal_places=13, null=True)
    count_down_p80                      = models.DecimalField(max_digits=16, decimal_places=13, null=True)
    count_down_p90                      = models.DecimalField(max_digits=16, decimal_places=13, null=True)
    count_up_p10                        = models.DecimalField(max_digits=16, decimal_places=13, null=True)
    count_up_p20                        = models.DecimalField(max_digits=16, decimal_places=13, null=True)
    count_up_p50                        = models.DecimalField(max_digits=16, decimal_places=13, null=True)
    count_up_p80                        = models.DecimalField(max_digits=16, decimal_places=13, null=True)
    count_up_p90                        = models.DecimalField(max_digits=16, decimal_places=13, null=True)
    density_avg_p20                     = models.FloatField(null=True)
    density_avg_p50                     = models.FloatField(null=True)
    density_avg_p80                     = models.FloatField(null=True)
    mf_insert_datetime                  = models.DateTimeField()
    mf_update_datetime                  = models.DateTimeField(null=True)
    mf_dp_available_datetime            = models.DateTimeField()
    mf_dp_changed_datetime              = models.DateTimeField(null=True)
    mf_row_hash                         = models.UUIDField(null=True)
    mf_dp_latest_ind                    = models.BooleanField(null=True)
    mf_deleted_ind                      = models.BooleanField(null=True)
    mf_run_id                           = models.IntegerField(null=True)

