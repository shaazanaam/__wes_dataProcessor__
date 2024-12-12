from django.urls import path
from . import views

urlpatterns = [
    path('', views.data_processor_home, name='data_processor_home'), ## Just a view page for the data processor view
    path('upload/', views.upload_file, name='upload_file'),
    path('success/', views.transformation_success, name='transformation_success'),
    path('download/', views.download_excel, name='download_excel'),  #  line to take to the download page
    path('download_csv/', views.download_csv, name='download_csv'),  # Add this line
]