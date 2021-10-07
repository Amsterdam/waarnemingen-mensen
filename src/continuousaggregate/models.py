from django.db import models

from contrib.timescale.fields import TimescaleDateTimeField


class Cmsa15Min(models.Model):
    bk_continuousaggregate_cmsa_15_min  = models.CharField(max_length=255) 
    sensor                              = models.CharField(max_length=255) 
    timestamp_rounded                   = TimescaleDateTimeField(interval="1 day")  # Timestamp of when the data was recorded in the camera
    total_count                         = models.IntegerField()
    count_down                          = models.IntegerField()
    count_up                            = models.IntegerField()
    density_avg                         = models.FloatField()
    basedonxmessages                    = models.IntegerField()
    total_count_p10                     = models.DecimalField(max_digits=16, decimal_places=13)
    total_count_p20                     = models.DecimalField(max_digits=16, decimal_places=13)
    total_count_p50                     = models.DecimalField(max_digits=16, decimal_places=13)
    total_count_p80                     = models.DecimalField(max_digits=16, decimal_places=13)
    total_count_p90                     = models.DecimalField(max_digits=16, decimal_places=13)
    count_down_p10                      = models.DecimalField(max_digits=16, decimal_places=13)
    count_down_p20                      = models.DecimalField(max_digits=16, decimal_places=13)
    count_down_p50                      = models.DecimalField(max_digits=16, decimal_places=13)
    count_down_p80                      = models.DecimalField(max_digits=16, decimal_places=13)
    count_down_p90                      = models.DecimalField(max_digits=16, decimal_places=13)
    count_up_p10                        = models.DecimalField(max_digits=16, decimal_places=13)
    count_up_p20                        = models.DecimalField(max_digits=16, decimal_places=13)
    count_up_p50                        = models.DecimalField(max_digits=16, decimal_places=13)
    count_up_p80                        = models.DecimalField(max_digits=16, decimal_places=13)
    count_up_p90                        = models.DecimalField(max_digits=16, decimal_places=13)
    density_avg_p20                     = models.FloatField()
    density_avg_p50                     = models.FloatField()
    density_avg_p80                     = models.FloatField()
    mf_insert_datetime                  = models.DateTimeField()
    mf_update_datetime                  = models.DateTimeField()
    mf_dp_available_datetime            = models.DateTimeField()
    mf_dp_changed_datetime              = models.DateTimeField()
    mf_row_hash                         = models.UUIDField()
    mf_dp_latest_ind                    = models.BooleanField()
    mf_deleted_ind                      = models.BooleanField()
    mf_run_id                           = models.IntegerField()

