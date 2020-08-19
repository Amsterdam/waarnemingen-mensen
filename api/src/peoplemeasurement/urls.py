from rest_framework import routers

from . import views

router = routers.DefaultRouter()
#router.register('15minaggregate', views.Today15minAggregationViewSet, basename='15minaggregate')
router.register('', views.PeopleMeasurementViewSet)

urlpatterns = router.urls
