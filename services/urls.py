from django.urls import path
from .views import service_tree_view

urlpatterns = [
    path('', service_tree_view, name='service_tree')
]