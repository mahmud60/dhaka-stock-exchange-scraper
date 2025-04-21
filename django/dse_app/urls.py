from django.urls import path
from . import views

urlpatterns = [
    path('', views.show_companies, name='company-list'),
]