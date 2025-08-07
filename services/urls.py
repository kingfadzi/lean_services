from django.urls import path
from .views import service_tree_view
from .tree_views import application_tree_view

urlpatterns = [
    path('', service_tree_view, name='service_tree'),
    path('application_tree/', application_tree_view, name='application_tree_view'),
]