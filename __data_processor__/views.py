from django.shortcuts import render, redirect
from django.db.utils import OperationalError
import time
import os
import csv
from django.urls import reverse  # For generating URLs
from .models import SchoolData ,TransformedSchoolData
from .forms import UploadFileForm
from django.http import HttpResponse
import logging
from django.conf import settings
logger = logging.getLogger(__name__)
import pandas as pd
from django.db import transaction
from django.core.paginator import Paginator
from django.contrib import messages  # For adding feedback messages
from .transformers import DataTransformer

def data_processor_home(request):
    if request.method == 'POST':
        # Check which transformation type was selected
        transformation_type = request.POST.get('transformation_type', 'Statewide')  # Default to 'Statewide'

         # Instantiate the DataTransformer and apply the transformation
        transformer = DataTransformer(request)
        success = transformer.apply_transformation(transformation_type)

        # If transformation was successful, redirect to success page
        if success:
            return redirect(f'/data_processor/success/?type={transformation_type}')

        # If transformation failed, stay on the same page to display the error
        return redirect('/data_processor/')  # Or render the page with the error message

    return render(request, 'index.html')

## Create a view and template to display a success message after the transformation 

def transformation_success(request):
    # Example details we can customize this based on the needs
    details = "Transformation Completed Successfully. Check the Updated records in the database"

    # Determine the transformation type from the query parameter (default to 'Statewide')
    transformation_type = request.GET.get('type', 'Statewide')  # Default to Statewide if not specified

    # Retrieve the appropriate transformed data based on the transformation type
    if transformation_type == 'Tri-County':
        data_list = TransformedSchoolData.objects.filter(place='Tri-County')
    else:
        data_list = TransformedSchoolData.objects.filter(place='WI')  # Default to Statewide if type is not 'Tri-County'
    
    # Paginate the results
    paginator = Paginator(data_list, 20)  # Show 20 records per page
    page_number = request.GET.get('page')  # Get the page number from the query parameters
    data = paginator.get_page(page_number)  # Get the page object

    # Return the rendered success page with the appropriate data
    return render(request, '__data_processor__/success.html', {
        'message': details,
        'data': data,  # This is the paginated data
        'transformation_type': transformation_type,  # The transformation type (Statewide or Tri-County)
    })

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

    # Process the file 
    retries = 5
    while retries > 0:
        try:
            with open(file_path, 'r') as file:
                reader = csv.DictReader(file)
                data = []

                #Clear the existing data in the SchoolData table to avoid duplicates
                # THIS ENSIRES THAT THE DATA BASE IS CLEANED BEFORE INSERTING NEW DATA
                SchoolData.objects.all().delete()

                #Parse the file and prepare the data for insertion
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
                
                #Insert new data into the database

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

# data_processor/views.py
def upload_file(request):
    message = ""  # Initialize the message variable
    form = UploadFileForm()  # Initialize the form

    if request.method == 'POST':
        # Handle file upload
        if request.FILES.get('file'):  # Check if a file is uploaded
            form = UploadFileForm(request.POST, request.FILES)
            if form.is_valid():
                handle_uploaded_file(request.FILES['file'])  # Process the file with the helper function
                # Redirect to the success page or back to upload with a success message
                return redirect(f"{reverse('upload')}?message=File uploaded successfully. Now you can run the transformation.")

        # Handle transformation actions
        transformation_type = request.POST.get('transformation_type')
        if transformation_type:
            transformer = DataTransformer(request)  # Create an instance of the DataTransformer class
            success = transformer.apply_transformation(transformation_type)  # Apply the transformation

            # If transformation was successful, redirect to the success page
            if success:
                return redirect(f"{reverse('transformation_success')}?type={transformation_type}")
            else:
                # If transformation failed, display an error message
                message = "Transformation failed. Please try again."

    else:
        form = UploadFileForm()  # Initialize the form if it's a GET request

    # Get any message passed via query parameters
    message = request.GET.get('message', message)  # Show the message if it exists
    details = "Upload your file and select the transformation type."
    return render(request, '__data_processor__/upload.html', {'form': form, 'message': message, 'details': details})


def statewide_view(request):
    transformation_type = request.GET.get('type')   # Default to 'Statewide' if not specified
    print(f"Query Parameters: {request.GET}")  # Log query parameters

    """ View to display the statewide data """
   # Simply fetching the transformed data from the data base 
    data_list = TransformedSchoolData.objects.filter(place='WI') # Assuming the place is WI
    paginator = Paginator(data_list, 20)  # Show 30 records per page
    page_number = request.GET.get('page')
    data = paginator.get_page(page_number)
    return render(request, '__data_processor__/statewide.html', {
        'data': data,
        'transformation_type': transformation_type,  # The transformation type (Statewide or Tri-County)
        })

def tri_county_view(request):
    transformation_type = request.GET.get('type')  # Log the type
    print(f"Query Parameters: {request.GET}")  # Log query parameters
    """ View to display the Tri-County data """
    data_list = TransformedSchoolData.objects.filter(place='Tri-County')
    paginator = Paginator(data_list, 20)  # Show 30 records per page
    page_number = request.GET.get('page')
    data = paginator.get_page(page_number)
    return render(request, '__data_processor__/tricounty.html', {
        'data': data,
        'transformation_type': transformation_type
    })

##EXCEL HANDLE ##

def generate_transformed_excel(transformation_type):
    # Fetch the transformed data based on the transformation type
    if transformation_type == 'Tri-County':
        data = TransformedSchoolData.objects.filter(place='Tri-County')
    else:
        data = TransformedSchoolData.objects.filter(place='WI')  # Default to Statewide if not Tri-County
    
    # Convert the QuerySet to a list of dictionaries
    data_list = list(data.values())

    # Create a Pandas DataFrame
    df = pd.DataFrame(data_list)

    # Generate the Excel file name
    excel_file = f'transformed_{transformation_type.lower()}_data.xlsx'
    
    # Use the context manager to handle the saving of the Excel file
    with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
        # Convert the DataFrame to an Excel object
        df.to_excel(writer, sheet_name='Transformed Data', index=False)

    # The file is automatically saved and closed when the context manager exits
    return excel_file

def download_excel(request):
    # Get the transformation type from the URL query parameter
    transformation_type = request.GET.get('type', 'Statewide')  # Default to 'Statewide' if not specified

    excel_file = generate_transformed_excel(transformation_type)

    # Serve the file as a download
    with open(excel_file, 'rb') as f:
        response = HttpResponse(f.read(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = f'attachment; filename={excel_file}'
        return response

## CSV HANDLE##


def generate_transformed_csv(transformation_type):
    # Fetch the transformed data based on the transformation type
    if transformation_type == 'Tri-County':
        data = TransformedSchoolData.objects.filter(place='Tri-County')
    else:
        data = TransformedSchoolData.objects.filter(place='WI')  # Default to Statewide if not Tri-County
    
    # Convert the QuerySet to a list of dictionaries and exclude the id field
    data_list = [
        {key.upper(): value for key, value in row.items() if key != 'id'}
        for row in data.values()
    ]
    
    # Get the field names (keys) from the first item, converted to uppercase
    fieldnames = data_list[0].keys() if data_list else []

    # Generate CSV file
    csv_file = f'transformed_{transformation_type.lower()}_data.csv'
    with open(csv_file, 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data_list)
    
    return csv_file

def download_csv(request):
    # Get the transformation type from the URL query parameter
    transformation_type = request.GET.get('type', 'Statewide')  # Default to 'Statewide' if not specified

    csv_file = generate_transformed_csv(transformation_type)

    # Serve the file as a download
    with open(csv_file, 'rb') as f:
        response = HttpResponse(f.read(), content_type='text/csv')
        response['Content-Disposition'] = f'attachment; filename={csv_file}'
        return response






# def upload_file(request):
#     message = ""  # Initialize the message variable
#     form = UploadFileForm()  # Initialize the form

#     if request.method == 'POST':
#         # Handle file upload
#         if request.FILES.get('file'):  # Check if a file is uploaded
#             form = UploadFileForm(request.POST, request.FILES)
#             if form.is_valid():
#                 handle_uploaded_file(request.FILES['file'])  # Assuming this function processes the file
#                 # Redirect to the success page or back to upload with a success message
#                 return redirect(f"{reverse('upload')}?message=File uploaded successfully. Now you can run the transformation.")

#         # Handle transformation actions
#         transformation_type = request.POST.get('transformation_type')
#         if transformation_type == 'Statewide':
#             transform_statwide_data(request)  # Trigger the statewide transformation function
#             return redirect(f"{reverse('transformation_success')}?type=Statewide")
#         elif transformation_type == 'Tri-County':
#             transform_Tri_County_data(request)  # Trigger the Tri-County transformation function
#             return redirect(f"{reverse('transformation_success')}?type=Tri-County")

#     else:
#         form = UploadFileForm()  # Initialize the form if it's a GET request

#     # Get any message passed via query parameters
#     message = request.GET.get('message', "")
#     return render(request, '__data_processor__/upload.html', {'form': form, 'message': message})
