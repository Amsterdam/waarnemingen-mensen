from rest_framework import routers

from . import views

router = routers.DefaultRouter()
router.register(
    "15minaggregate", views.Today15minAggregationViewSet, basename="15minaggregate"
)
router.register("sensor", views.SensorsDataViewSet, basename="sensor")
router.register("servicelevel", views.ServicelevelDataViewSet, basename="servicelevel")

urlpatterns = router.urls
