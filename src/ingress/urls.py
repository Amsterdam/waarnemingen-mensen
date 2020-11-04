from django.urls import path
from ingress.views import IngressView

urlpatterns = [
    path(f'<queue>', IngressView.as_view()),
]
