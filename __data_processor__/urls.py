from django.urls import path
from . import views

urlpatterns = [
    path('', views.data_processor_home, name='data_processor_home'),
    path('upload/', views.upload_file, name='upload'),
    path('success/', views.transformation_success, name='transformation_success'),
    path('download/', views.download_excel, name='download_excel'),
    path('download_csv/', views.download_csv, name='download_csv'),
    path('statewide/', views.statewide_view, name='statewide_view'),
    path('tricounty/', views.tri_county_view, name='tri_county_view'),
    path('county_layer/', views.county_layer_view, name='county_layer_view'),
    path('metopio_statewide/', views.metopio_statewide_view, name='metopio_statewide_layer_view'),
    path('metopio_zipcode/', views.metopio_zipcode_view, name='metopio_zipcode_layer_view'),

    

]
