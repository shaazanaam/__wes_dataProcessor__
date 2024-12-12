from django.shortcuts import render, redirect
from django.db.utils import OperationalError
import time
import os
import csv
from django.shortcuts import render
from .models import SchoolData ,TransformedSchoolData
from .forms import UploadFileForm
from django.http import HttpResponse
import logging
from django.conf import settings
logger = logging.getLogger(__name__)
import pandas as pd
from django.db import transaction


def home(request):
    return HttpResponse("""
        <html>
        <head><title>Home Page</title></head>
        <body>
            <h1>Welcome to the School Data Processor!</h1>
            <a href="/data_processor/">Go to Data Processor Home</a>
        </body>
        </html>
    """)


def data_processor_home(request):
    if request.method == 'POST':
        transform_data()
        return redirect('transformation_success')
    return render(request, '__data_processor__/home.html')

## Create a view and template to display a success message after the transformation 
def transformation_success(request):
    # Example details we can customize this based on the needs
    details = "Transformation Completed Successfully. Check the Updated records in the database"
    return render(request, '__data_processor__/success.html',{'message':'Transformation Completed Successfully!'})



def handle_uploaded_file(f):
    #Save to a Directory in Your Project: Create a directory within your project to 
    # store uploaded files. For example, create a directory called uploads in your project root.
    upload_dir = os.path.join(settings.BASE_DIR, 'uploads')
    os.makedirs(upload_dir, exist_ok=True)
    file_path = os.path.join(upload_dir, f.name)
    
    with open(file_path, 'wb+') as destination:
        for chunk in f.chunks():
            destination.write(chunk)
    logger.info(f"File uploaded successfully to {file_path}")

    # Process the file as before
    retries = 5
    while retries > 0:
        try:
            with open(file_path, 'r') as file:
                reader = csv.DictReader(file)
                data = []
                for row in reader:
                    if row['STUDENT_COUNT'] == '*':
                        continue
                    data.append(SchoolData(
                        school_year=row['SCHOOL_YEAR'],
                        agency_type=row['AGENCY_TYPE'],
                        cesa=row['CESA'],
                        county=row['COUNTY'],
                        district_code=row['DISTRICT_CODE'],
                        school_code=row['SCHOOL_CODE'],
                        grade_group=row['GRADE_GROUP'],
                        charter_ind=row['CHARTER_IND'],
                        district_name=row['DISTRICT_NAME'],
                        school_name=row['SCHOOL_NAME'],
                        group_by=row['GROUP_BY'],
                        group_by_value=row['GROUP_BY_VALUE'],
                        student_count=row['STUDENT_COUNT'],
                        percent_of_group=row['PERCENT_OF_GROUP']
                    ))
                SchoolData.objects.bulk_create(data)
                logger.info(f"{len(data)} records inserted into the database")
                break
        except OperationalError as e:
            if 'database is locked' in str(e):
                retries -= 1
                logger.warning(f"Database is locked, retrying... ({5 - retries} retries left)")
                time.sleep(1)  # Wait for 1 second before retrying
            else:
                logger.error(f"Error processing file: {e}")
                raise

def upload_file(request):
    if request.method == 'POST':
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            handle_uploaded_file(request.FILES['file'])
            return render(request, '__data_processor__/success.html')
    else:
        form = UploadFileForm()
    return render(request, '__data_processor__/upload.html', {'form': form})



### Sample Transformation Logic
def transform_data():
    # Define the mapping dictionaries
    school_code_to_city = {
        '1234': 'Appleton',
        '5678': 'Oshkosh',
        # Add more mappings
    }

    school_code_to_zip = {
        '1234': '54911',
        '5678': '54901',
        # Add more mappings
    }

    data = SchoolData.objects.all().exclude(student_count='*')

    transformed_data = []
    for entry in data:
        transformed_entry = TransformedSchoolData(
            school_year=entry.school_year,
            agency_type=entry.agency_type,
            cesa=entry.cesa,
            county=entry.county,
            district_code=entry.district_code,
            school_code=entry.school_code,
            grade_group=entry.grade_group,
            charter_ind=entry.charter_ind,
            district_name=entry.district_name,
            school_name=entry.school_name,
            group_by=entry.group_by,
            group_by_value=entry.group_by_value,
            student_count=entry.student_count,
            percent_of_group=entry.percent_of_group
        )
        # Apply transformations to transformed_entry.place
        if entry.school_name == '[Statewide]':
            transformed_entry.place = 'WI'
        elif entry.county in ['Outagamie', 'Winnebago', 'Calumet'] and entry.school_name == '[Districtwide]':
            transformed_entry.place = 'Tri-County'
        elif entry.county in ['Outagamie', 'Winnebago', 'Calumet'] and entry.school_name == '[Districtwide]':
            transformed_entry.place = f"{entry.county}, WI"
        elif entry.county in ['Outagamie', 'Winnebago', 'Calumet'] and not entry.school_name.startswith('['):
            city_name = school_code_to_city.get(entry.school_code, 'Unknown City')
            transformed_entry.place = f"{city_name}, WI"
        elif entry.county in ['Outagamie', 'Winnebago', 'Calumet'] and not entry.school_name.startswith('['):
            zip_code_value = school_code_to_zip.get(entry.school_code, 'Unknown Zip')
            transformed_entry.place = zip_code_value
        elif entry.county in ['Outagamie', 'Winnebago', 'Calumet'] and not entry.school_name.startswith('['):
            transformed_entry.place = "School District"
        # Add other transformations as needed
        transformed_data.append(transformed_entry)

    TransformedSchoolData.objects.bulk_create(transformed_data)

def generate_excel():
    # Fetch the transformed data from the database
    data = TransformedSchoolData.objects.all().values()
    df = pd.DataFrame(data)

    # Create a Pandas Excel writer using XlsxWriter as the engine
    excel_file = 'transformed_data.xlsx'
    writer = pd.ExcelWriter(excel_file, engine='xlsxwriter')

    # Convert the dataframe to an XlsxWriter Excel object
    df.to_excel(writer, sheet_name='Transformed Data', index=False)

    # Close the Pandas Excel writer and output the Excel file
    writer.close()

    return excel_file

def download_excel(request):
    excel_file = generate_excel()

    # Serve the file as a download
    with open(excel_file, 'rb') as f:
        response = HttpResponse(f.read(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = f'attachment; filename={excel_file}'
        return response

import csv

def generate_csv():
    # Fetch the transformed data from the database
    data = TransformedSchoolData.objects.all().values()
    csv_file = 'transformed_data.csv'

    # Write the data to a CSV file
    with open(csv_file, 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        writer.writeheader()
        for row in data:
            writer.writerow(row)

    return csv_file

def download_csv(request):
    csv_file = generate_csv()

    # Serve the file as a download
    with open(csv_file, 'rb') as f:
        response = HttpResponse(f.read(), content_type='text/csv')
        response['Content-Disposition'] = f'attachment; filename={csv_file}'
        return response