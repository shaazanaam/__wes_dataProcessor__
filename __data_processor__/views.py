from django.shortcuts import render, redirect
from django.db.utils import OperationalError
import time
import os
import csv
from django.urls import reverse  # For generating URLs
from .models import (
    SchoolData,
    TransformedSchoolData,
    Stratification,
    MetopioTriCountyLayerTransformation,
    CountyLayerTransformation,
    MetopioStateWideLayerTransformation,
    ZipCodeLayerTransformation,
    MetopioCityLayerTransformation,  # Add this line
)
from .forms import UploadFileForm
from .models import ZipCodeLayerTransformation
from .models import SchoolAddressFile
from .models import CountyGEOID
from django.http import HttpResponse
import logging
from django.conf import settings

logger = logging.getLogger(__name__)
import pandas as pd
from django.db import transaction, connection
from django.core.paginator import Paginator
from django.contrib import messages  # For adding feedback messages
from .transformers import DataTransformer



def data_processor_home(request):
    if request.method == "POST":
        # Check which transformation type was selected
        transformation_type = request.POST.get(
            "transformation_type", "Statewide V01"
        )  # Default to 'Statewide'
        # Instantiate the DataTransformer and apply the transformation
        transformer = DataTransformer(request)
        success = transformer.apply_transformation(transformation_type)

        # If transformation was successful, redirect to success page
        if success:
            return redirect(f"/data_processor/success/?type={transformation_type}")

        # If transformation failed, stay on the same page to display the error
        return redirect("/data_processor/")  # Or render the page with the error message

    return render(request, "index.html")


## Create a view and template to display a success message after the transformation


def transformation_success(request):
    # Example details we can customize this based on the needs
    details = "Transformation Completed Successfully. Check the Updated records in the database"

    # Determine the transformation type from the query parameter (default to 'Statewide')
    transformation_type = request.GET.get(
        "type", "Statewide V01"
    )  # Default to Statewide if not specified
    # Instantiate DataTransformer properly with the request
    # Transformer = DataTransformer(request)
    # Retrieve the appropriate transformed data based on the transformation type
    # Run the transformation explicitly
    if transformation_type == "Statewide V01":
        transformer = DataTransformer(request)
        transformer.apply_transformation("Statewide V01")
        data_list = TransformedSchoolData.objects.filter(place="WI")
        return redirect(
            reverse("statewide_view")
        )  # Replace 'statewide_view' with the actual name of your URL
    elif transformation_type == "Tri-County":
        transformer = DataTransformer(request)
        transformer.apply_tri_county_layer_transformation()
        data_list = MetopioTriCountyLayerTransformation.objects.all()
    elif transformation_type == "County-Layer":
        transformer = DataTransformer(request)  # Apply County Layer transformation
        transformer.apply_county_layer_transformation()
        data_list = CountyLayerTransformation.objects.all()
    elif transformation_type == "Metopio Statewide":
        transformer = DataTransformer(request)
        transformer.transform_Metopio_StateWideLayer()
        data_list = MetopioStateWideLayerTransformation.objects.all()
    elif transformation_type == "Zipcode":
        transformer = DataTransformer(request)
        transformer.transforms_Metopio_ZipCodeLayer()
        data_list = ZipCodeLayerTransformation.objects.all()
    elif transformation_type == "City-Town":
        transformer = DataTransformer(request)
        transformer.transform_Metopio_CityLayer()
        data_list = MetopioCityLayerTransformation.objects.all()
    else:
        # Handle unknown transformation types
        details = "Unknown transformation type. Please check your request."
        data_list = []

    

    # Paginate the results
    paginator = Paginator(data_list, 20)  # Show 20 records per page
    page_number = request.GET.get(
        "page"
    )  # Get the page number from the query parameters
    data = paginator.get_page(page_number)  # Get the page object

    # Return the rendered success page with the appropriate data
    return render(
        request,
        "__data_processor__/success.html",
        {
            "message": details,
            "data": data,  # This is the paginated data
            "transformation_type": transformation_type,  # The transformation type (Statewide or Tri-County)
        },
    )


# Key features of the Function Handle_uploaded_file
# Handles two file upload : Saves the uploaded files to a consistent directory in the project
# Processes two files : Supports the processing of a main data file and an optional stratification file
# Uses Bulk Operations ; Employs bulk insertion to to efficiently save data
# Handles the error gracefully : Implements the retry mechanism to handle database locking errors
# Links the data in the SchoolData and the Stratification using the foreign key

# handling to load the main file and the stratification file
def handle_uploaded_file(f, stratifications_file=None):
    # Save to a Directory in Your Project: Create a directory within your project to
    # store uploaded files. For example, create a directory called uploads in your project root.
    upload_dir = os.path.join(settings.BASE_DIR, "uploads")
    os.makedirs(upload_dir, exist_ok=True)
    file_path = os.path.join(upload_dir, f.name)

    # Save  and process the Main File
    with open(file_path, "wb+") as destination:
        for chunk in f.chunks():
            destination.write(chunk)
    logger.info(f"File uploaded successfully to {file_path}")

    #  If the stratification file is provided process it and Save it to the uploads directory
    strat_map = {}
    if stratifications_file:
        strat_file_path = os.path.join(upload_dir, stratifications_file.name)
        with open(strat_file_path, "wb+") as destination:
            for chunk in stratifications_file.chunks():
                destination.write(chunk)
        logger.info(f"Stratification file uploaded successfully to {strat_file_path}")

        # Load stratifications into the database
        try:
            with open(strat_file_path, "r") as strat_file:
                strat_reader = csv.DictReader(strat_file)
                Stratification.objects.all().delete()  # Clear existing stratifications
                for row in strat_reader:
                    group_by = row["GROUP_BY"]
                    group_by_value = row["GROUP_BY_VALUE"]
                    label_name = row["Stratification"]

                    # creates Stratification object in the database
                    # for each unique combination of group_by and group_by_value

                    strat, created = Stratification.objects.get_or_create(
                        group_by=group_by,
                        group_by_value=group_by_value,
                        label_name=label_name,
                    )
                    # Builds a dictionay(strat_map) mapping the combination of the group_by and group_by_value
                    # to the label_name
                    # This will be used to assign the stratification to the SchoolData objects

                    strat_map[f"{group_by}{group_by_value}"] = strat.label_name
        except Exception as e:
            logger.error(f"Error processing stratifications file: {e}")
            raise

    # Process the Main file  to write to the data base
    retries = 5
    while retries > 0:
        try:
            with open(file_path, "r") as file:
                reader = csv.DictReader(file)
                # Clear the existing data in the SchoolData table to avoid duplicates
                # THIS ENSIRES THAT THE DATA BASE IS CLEANED BEFORE INSERTING NEW DATA
                SchoolData.objects.all().delete()
                data = []

                # Builds a dictionary(strat_map) mapping  each combination of group_by and 
                # group_by_value
                # from the Stratification model to its corresponding Stratification object

                strat_map = {
                    f"{strat.group_by}{strat.group_by_value}": strat
                    for strat in Stratification.objects.all()
                }

                # Parse the file and prepare the data for insertion
                # Opens the main file and then constructs a key ( combined_key)
                # by concatenating the GROUP_BY and the GROUP_BY_VALUE for each row
                # uses the strat_map to find the coreesponding Stratification object for the combined_key
                # and then creates a SchoolData object with the corresponding Stratification object
                # and appends it to the data list
                # The data list is then bulk inserted into the SchoolData table
                # This ensures that the data is inserted in a single transaction
                for row in reader:
                    combined_key = row["GROUP_BY"] + row["GROUP_BY_VALUE"]
                    stratification = strat_map.get(combined_key)

                    if (
                        row["STUDENT_COUNT"] == "*"
                        or row["GROUP_BY_VALUE"] == "[Data Suppressed]"
                    ):
                        continue
                    data.append(
                        SchoolData(
                            school_year=row["SCHOOL_YEAR"],
                            agency_type=row["AGENCY_TYPE"],
                            cesa=row["CESA"],
                            county=row["COUNTY"],
                            district_code=row["DISTRICT_CODE"],
                            school_code=row["SCHOOL_CODE"],
                            grade_group=row["GRADE_GROUP"],
                            charter_ind=row["CHARTER_IND"],
                            district_name=row["DISTRICT_NAME"],
                            school_name=row["SCHOOL_NAME"],
                            group_by=row["GROUP_BY"],
                            group_by_value=row["GROUP_BY_VALUE"],
                            student_count=row["STUDENT_COUNT"],
                            percent_of_group=row["PERCENT_OF_GROUP"],
                            stratification=stratification, 
                            # stratification field is set to the corresponding Stratification object
                            # for the combined_key (group_by + group_by_value) if it exists in the strat_map
                            # otherwise it is set to None (NULL in the database table SchoolData) 
                            # This ensures that the stratification field is properly linked to the Stratification model
                            # using the foreign key relationship (stratification_id in the SchoolData table)
                            # This is a Many-to-One relationship between the SchoolData and Stratification models
                            # where each SchoolData object can have only one Stratification object
                        )
                    )

                # Insert new data into the database

                SchoolData.objects.bulk_create(data)
                logger.info(f"{len(data)} records inserted into the database")
                break
        except OperationalError as e:
            if "database is locked" in str(e):
                retries -= 1
                logger.warning(
                    f"Database is locked, retrying... ({5 - retries} retries left)"
                )
                time.sleep(1)  # Wait for 1 second before retrying
            else:
                logger.error(f"Error processing file: {e}")
                raise


# data_processor/views.py
def upload_file(request):
    message = ""             # Initialize the message variable
    form = UploadFileForm()  # Initialize the form

    if request.method == "POST":
        # Handle file upload
        file = request.FILES.get("file")
        stratifications_file = request.FILES.get("stratifications_file")  
        county_geoid_file = request.FILES.get("county_geoid_file") #New County GEOID file
        school_address_file = request.FILES.get("school_address_file")  #New School Address file
        
        
        #Get the stratification file if provided

        if file:  # Check if a file is uploaded
            form = UploadFileForm(request.POST, request.FILES)
            if form.is_valid():
                handle_uploaded_file(
                    file, 
                    stratifications_file=stratifications_file,
                    )   # Process the main file and the stratification uploaded file
            
        #Process the COunty GEOID file if provided
        if county_geoid_file:
            load_county_geoid_file(county_geoid_file)
        if school_address_file:
            load_school_address_file(school_address_file)
            # after loading the school_address_file we are going to  update the SchoolData model
            # to take care of the Many-to-Many relationship
            # We will be tryng to handle the Many to Many relationship population of the SchoolData model
            # by bypassing  the bulk updates and rather use the native SQL for batch processing
            
            try:
                logger.info("Populating address_details for the SchoolData model...")

                # Fetch all the necessary data upfront
                school_address_data = SchoolAddressFile.objects.values("id", "lea_code", "school_code")
                school_data = SchoolData.objects.values("id", "district_code", "school_code")

                # Create a dictionary mapping (lea_code, school_code) from the  SchoolAddressFile ID
                
                address_map = {
                    (address["lea_code"], address["school_code"]): address["id"]
                    for address in school_address_data
                }

                # Prepare records for the intermediate table
                # Please notice that we are not going to be inserting the entire school object or the complete dictionary
                # of the school data -- we are just inserting  the value corresponding to id key in the school dictionary
                # What then goes into the records_to_insert is the id of the school and the id of the address
                # records_to_insert =  is a list of tuples containing the school["id"] and the address_id
                # please also note that both the school["id"] and the address_id are the primary keys 
                # of the corresponding school records in the SchoolData and SchoolAddressFile models respectively
                # Why only ids are Inserted:
                # The Many-to-Many relationship between the SchoolData and SchoolAddressFile  is being stored in an intermediate 
                # table that Django creates automatically. This table has two columns: schooldata_id and schooladdressfile_id.
                # this table only needs to know which schooldata_id( the id of the school) corresponds to which 
                # schooladdressfile_id (the id of the address).
                # Storing only the id values ensures minimal redundancy and optimal storage.
                # The id value in the school_data dictionary
                # ({'id': 2080317, 'district_code': '8022', 'school_code': '8148'}) 
                # represents the primary key of the corresponding SchoolData object
                # Please also understand that key for the  dictioary for the address map ais actually the tuple of the lea_code and the school_code
                # so the address_map.get((school["district_code"], school["school_code"])) will return the id of the address
                
                
                m2m_table_name = SchoolData.address_details.through._meta.db_table
                records_to_insert = []

                for school in school_data:
                    address_id = address_map.get((school["district_code"], school["school_code"]))
                    if address_id:
                        records_to_insert.append((school["id"], address_id))

                # Delete existing Many-to-Many relationships
                with connection.cursor() as cursor:
                    cursor.execute(f"DELETE FROM {m2m_table_name}")

                # Insert new relationships in batches
                # values =", ".join(f"({school_id}, {address_id})" for school_id, address_id in records_to_insert)
                # Converts the batch into a string representation for the SQL insert query
                # for example for a batch of [(1, 2), (3, 4), (5, 6)]
                # the values will be "(1, 2), (3, 4), (5, 6)"
                # then cursor.execute(f"INSERT INTO {m2m_table_name} (schooldata_id, schooladdressfile_id) VALUES {values}")
                # inserts the values into the intermediate table M2M  join table
                # for a batch of 500 records at a time this means inserting 500 rows in a single query
                
                batch_size = 500
                with connection.cursor() as cursor:
                    for i in range(0, len(records_to_insert), batch_size):
                        batch = records_to_insert[i:i+batch_size]
                        values = ", ".join(f"({school_id}, {address_id})" for school_id, address_id in batch)
                        cursor.execute(f"INSERT INTO {m2m_table_name} (schooldata_id, schooladdressfile_id) VALUES {values}")

                logger.info(f"Successfully populated {len(records_to_insert)} address details for SchoolData records.")

            except Exception as e:
                logger.error(f"Error populating address details: {e}")
                raise   
            # Redirect to the success page or back to upload with a success message

            return redirect(
                f"{reverse('upload')}?message=File uploaded successfully. Now you can run the transformation."
            )
        # After this step in the rendered page we have the transformation forms from where we can get the tranformation type
        # since we called the handle_uploaded_file function to process the file and save it to the database
        # we are already prepared to handle the transformation actions after the page as above has been rendered
        # Handle transformation actions
        transformation_type = request.POST.get("transformation_type")
        if transformation_type:
            transformer = DataTransformer(request)  # Create an instance of the DataTransformer class
            if transformation_type == "Tri-County":
                success = (transformer.apply_tri_county_layer_transformation())  # Apply the Tri-County Layer transformation
            elif transformation_type == "County-Layer":
                success = transformer.apply_county_layer_transformation()        # Apply County Layer transformation
            elif transformation_type == "Metopio Statewide":
                success = transformer.transform_Metopio_StateWideLayer()         # Apply Metopio Statewide transformation
            elif transformation_type == "Zipcode":
                success = transformer.transforms_Metopio_ZipCodeLayer()          # Apply Metopio Zipcode transformation 
            elif transformation_type == "City-Town":
                success = transformer.transform_Metopio_CityLayer()              # Apply Metopio City-Town transformation
            else:
                success = transformer.apply_transformation(transformation_type ) # Apply the transformation

            # If transformation was successful, redirect to the success page
            if success:
                return redirect(
                    f"{reverse('transformation_success')}?type={transformation_type}"
                )
            else:
                # If transformation failed, display an error message
                message = "Transformation failed. Please try again."

    else:
        form = UploadFileForm()  # Initialize the form if it's a GET request

    # Get any message passed via query parameters
    message = request.GET.get("message", message)  # Show the message if it exists
    details = "Upload your file and select the transformation type."
    return render(
        request,
        "__data_processor__/upload.html",
        {"form": form, "message": message, "details": details},
    )
    

# handle the county geoid file upload

def load_county_geoid_file(file):
    # Save the file to the uploads directory
    upload_dir = os.path.join(settings.BASE_DIR, "uploads")
    os.makedirs(upload_dir, exist_ok=True)
    file_path = os.path.join(upload_dir, file.name)

    with open(file_path, "wb+") as destination:
        for chunk in file.chunks():
            destination.write(chunk)
    logger.info(f"County GEOID file uploaded successfully to {file_path}")

    try:
        with open(file_path, "r") as csvfile:
            reader = csv.DictReader(csvfile)
            CountyGEOID.objects.all().delete()  # Clear existing records
            # Validate required columns
            required_columns = {"Layer", "Name", "GEOID"}
            if not required_columns.issubset(reader.fieldnames):
                raise ValueError(f"Missing required columns: {required_columns - set(reader.fieldnames)}")

           

            # Filter rows where Layer = 'County' and prepare data
            data = [
                CountyGEOID(
                    layer=row["Layer"],  # Use the "Layer" field
                    name=row["Name"],    # Use the "Name" field
                    geoid=row["GEOID"]   # Use the "GEOID" field
                )
                for row in reader 
            ]

            # Bulk insert filtered data
            CountyGEOID.objects.bulk_create(data)
            logger.info(f"{len(data)} County GEOID records inserted into the database")

    except Exception as e:
        logger.error(f"Error processing County GEOID file: {e}")
        raise

# handle the school AddressFile upload
def load_school_address_file(file):
    # Save the file to the uploads directory
    upload_dir = os.path.join(settings.BASE_DIR, "uploads")
    os.makedirs(upload_dir, exist_ok=True)
    file_path = os.path.join(upload_dir, file.name)

    with open(file_path, "wb+") as destination:
        for chunk in file.chunks():
            destination.write(chunk)
    logger.info(f"School Address file uploaded successfully to {file_path}")

    try:
        with open(file_path, "r") as csvfile:
            reader = csv.DictReader(csvfile)
            SchoolAddressFile.objects.all().delete()  # Clear existing records
            # Validate required columns
            required_columns = {
                "LEA Code", "District Name", "School Code", "School Name",
                "Organization Type", "School Type", "Low Grade", "High Grade",
                "Address", "City", "State", "Zip", "CESA", "Locale",
                "County", "Current Status", "Categories And Programs",
                "Virtual School", "IB Program", "Phone Number",
                "Fax Number", "Charter Status", "Website Url"
            }
            if not required_columns.issubset(reader.fieldnames):
                raise ValueError(f"Missing required columns: {required_columns - set(reader.fieldnames)}")
            
            with transaction.atomic():
                

                # Prepare data for bulk insertion
                data = [
                    SchoolAddressFile(
                        lea_code=row["LEA Code"],
                        district_name=row["District Name"],
                        school_code=row["School Code"],
                        school_name=row["School Name"],
                        organization_type=row["Organization Type"],
                        school_type=row["School Type"],
                        low_grade=row["Low Grade"],
                        high_grade=row["High Grade"],
                        address=row["Address"],
                        city=row["City"],
                        state=row["State"],
                        zip_code=row["Zip"],
                        cesa=row["CESA"],
                        locale=row["Locale"],
                        county=row["County"],
                        current_status=row["Current Status"],
                        categories_and_programs=row.get("Categories And Programs", ""),
                        virtual_school=row.get("Virtual School", ""),
                        ib_program=row.get("IB Program", ""),
                        phone_number=row["Phone Number"],
                        fax_number=row.get("Fax Number", ""),
                        charter_status=row["Charter Status"].lower() == "true",
                        website_url=row.get("Website Url", ""),
                    )
                    for row in reader
                ]

                # Bulk insert data
                SchoolAddressFile.objects.bulk_create(data)
            logger.info(f"{len(data)} School Address records inserted into the database")

    except Exception as e:
        logger.error(f"Error processing School Address file: {e}")
        raise

def statewide_view(request):
    transformation_type = request.GET.get(
        "type"
    )  # Default to 'Statewide' if not specified
    print(f"Query Parameters: {request.GET}")  # Log query parameters

    """ View to display the statewide data """
    # Simply fetching the transformed data from the data base
    data_list = TransformedSchoolData.objects.filter(
        place="WI"
    )  # Assuming the place is WI
    paginator = Paginator(data_list, 20)  # Show 30 records per page
    page_number = request.GET.get("page")
    data = paginator.get_page(page_number)
    return render(
        request,
        "__data_processor__/statewide.html",
        {
            "data": data,
            "transformation_type": transformation_type,  # The transformation type (Statewide or Tri-County)
        },
    )

def tri_county_view(request):
    transformation_type = request.GET.get(
        "type", "Tri-County"
    )  # Default to the TriCountry Layer if not specified
    print(f"Query Parameters: {request.GET}")  # Log query parameters
    # Fetch the data from the Metopio Data Transformation model
    DataTransformer.apply_tri_county_layer_transformation(request)
    data_list = MetopioTriCountyLayerTransformation.objects.all()
    """ View to display the Tri-County data """

    # Pagiante the Results
    paginator = Paginator(data_list, 20)  # Show 30 records per page
    page_number = request.GET.get("page")
    data = paginator.get_page(page_number)

    # pass the data to the template file
    return render(
        request,
        "__data_processor__/tricounty.html",
        {"data": data, "transformation_type": transformation_type},
    )

#COUNTY LAYER VIEW

# views.py

def county_layer_view(request):
    # Get the transformation type from the query parameters
    transformation_type = request.GET.get(
        "type", "County-Layer"
    )  # Default to County Layer if not specified
    print(f"Query Parameters: {request.GET}")  # Log query parameters

    # Apply the County Layer Transformation
    DataTransformer(request).apply_county_layer_transformation()

    # Fetch the transformed data from the CountyLayerTransformation model
    data_list = CountyLayerTransformation.objects.all()

    # Paginate the Results
    paginator = Paginator(data_list, 20)  # Show 20 records per page
    page_number = request.GET.get("page")
    data = paginator.get_page(page_number)

    # Pass the data to the template file
    return render(
        request,
        "__data_processor__/county_layer.html",
        {"data": data, "transformation_type": transformation_type},
    )

#METOPIO STATEWIDE VIEW
def metopio_statewide_view(request):
    # Get the transformation type from the query parameters
    transformation_type = request.GET.get(
        "type", "Statewide"
    )  # Default to 'Statewide' if not specified
    print(f"Query Parameters: {request.GET}")  # Log query parameters

    # Apply the Metopio Statewide Transformation
    DataTransformer(request).transform_Metopio_StateWideLayer()

    # Fetch the transformed data from the MetopioStateWideLayerTransformation model
    data_list = MetopioStateWideLayerTransformation.objects.all()

    # Paginate the Results
    paginator = Paginator(data_list, 20)  # Show 20 records per page
    page_number = request.GET.get("page")
    data = paginator.get_page(page_number)

    # Pass the data to the template file
    return render(
        request,
        "__data_processor__/metopio_statewide.html",
        {"data": data, "transformation_type": transformation_type},
    )

#METOPIO ZIPCODE VIEW
def metopio_zipcode_view(request):
    # Get the transformation type from the query parameters
    transformation_type = request.GET.get(
        "type", "Zipcode"
    )  # Default to 'Zipcode' if not specified
    print(f"Query Parameters: {request.GET}")  # Log query parameters

    # Apply the Metopio Zipcode Transformation
    DataTransformer(request).transforms_Metopio_ZipCodeLayer()

    # Fetch the transformed data from the MetopioZipCodeLayerTransformation model
    data_list = ZipCodeLayerTransformation.objects.all()

    # Paginate the Results
    paginator = Paginator(data_list, 20)  # Show 20 records per page
    page_number = request.GET.get("page")
    data = paginator.get_page(page_number)

    # Pass the data to the template file
    return render(
        request,
        "__data_processor__/metopio_zipcode.html",
        {"data": data, "transformation_type": transformation_type},
    )

#City or Town View

def city_town_view(request):
    transformation_type = request.GET.get(
        "type", "City-Town"
    )  # Default to 'City-Town' if not specified
    print(f"Query Parameters: {request.GET}")  # Log query parameters
    # Fetch the data from the Metopio Data Transformation model
    DataTransformer.transform_Metopio_CityLayer(request)
    data_list = MetopioCityLayerTransformation.objects.all()
    """ View to display the City-Town data """

    # Pagiante the Results
    paginator = Paginator(data_list, 20)  # Show 30 records per page
    page_number = request.GET.get("page")
    data = paginator.get_page(page_number)

    # pass the data to the template file
    return render(
        request,
        "__data_processor__/city_town.html",
        {"data": data, "transformation_type": transformation_type},
    )
#OUTPUT DOWNLOADS
##EXCEL HANDLE ##

def generate_transformed_excel(transformation_type):
    # Fetch the transformed data based on the transformation type
    if transformation_type == "Tri-County":
        data = MetopioTriCountyLayerTransformation.objects.all()
    else:
        data = TransformedSchoolData.objects.filter(
            place="WI"
        )  # Default to Statewide if not Tri-County

    # Convert the QuerySet to a list of dictionaries
    data_list = list(data.values())

    # Create a Pandas DataFrame
    df = pd.DataFrame(data_list)

    # Generate the Excel file name
    excel_file = f"transformed_{transformation_type.lower()}_data.xlsx"

    # Use the context manager to handle the saving of the Excel file
    with pd.ExcelWriter(excel_file, engine="xlsxwriter") as writer:
        # Convert the DataFrame to an Excel object
        df.to_excel(writer, sheet_name="Transformed Data", index=False)

    # The file is automatically saved and closed when the context manager exits
    return excel_file

def download_excel(request):
    # Get the transformation type from the URL query parameter
    transformation_type = request.GET.get(
        "type", "Statewide"
    )  # Default to 'Statewide' if not specified

    excel_file = generate_transformed_excel(transformation_type)

    # Serve the file as a download
    with open(excel_file, "rb") as f:
        response = HttpResponse(
            f.read(),
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        response["Content-Disposition"] = f"attachment; filename={excel_file}"
        return response

## CSV HANDLE##

def generate_transformed_csv(transformation_type):

    # Fetch the transformed data based on the transformation type
    # This is where you will be having various trnasformation types
    if transformation_type == "Tri-County":
        data = MetopioTriCountyLayerTransformation.objects.all()
    elif transformation_type == "County-Layer":
        data = CountyLayerTransformation.objects.all()
    elif transformation_type == "Metopio Statewide":
        data = MetopioStateWideLayerTransformation.objects.all()
    elif transformation_type == "Zipcode":
        data = ZipCodeLayerTransformation.objects.all()
    elif transformation_type == "City-Town":
        data = MetopioCityLayerTransformation.objects.all()
    else:
        data = TransformedSchoolData.objects.filter(
            place="WI"
        )  # Default to Statewide if not Tri-County

    # Convert the QuerySet to a list of dictionaries and exclude the id field
    data_list = [
        {key.lower(): value for key, value in row.items() if key != "id"}
        for row in data.values()
    ]

    # Get the field names (keys) from the first item, converted to uppercase and now lower case
    fieldnames = data_list[0].keys() if data_list else []

    # Generate CSV file
    csv_file = f"transformed_{transformation_type.lower()}_data.csv"
    with open(csv_file, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data_list)

    return csv_file

def download_csv(request):
    # Get the transformation type from the URL query parameter
    transformation_type = request.GET.get(
        "type", "Statewide"
    )  # Default to 'Statewide' if not specified

    csv_file = generate_transformed_csv(transformation_type)

    # Serve the file as a download
    with open(csv_file, "rb") as f:
        response = HttpResponse(f.read(), content_type="text/csv")
        response["Content-Disposition"] = f"attachment; filename={csv_file}"
        return response
