
# data_processor/transformers.py

from .models import (
    SchoolData,
    TransformedSchoolData, 
    MetopioTriCountyLayerTransformation, 
    CountyLayerTransformation,
    CountyGEOID, 
    MetopioStateWideLayerTransformation, 
    ZipCodeLayerTransformation,
    SchoolAddressFile,
    MetopioCityLayerTransformation,
    Stratification
)


from django.db import transaction
from django.db import models
import logging
import traceback
from django.db.models import Q, F
from django.contrib import messages
logger = logging.getLogger(__name__)
class DataTransformer:
    def __init__(self, request):
        self.request = request

    def transform_statewide(self):
        """ Transform 'Statewide' data from the SchoolData model """
        if not SchoolData.objects.exists():
            messages.error(self.request, 'No data found in the SchoolData model. Please upload a file first.')
            return False  # Indicate failure

        # Clear existing data in the TransformedSchoolData to avoid duplicates
        TransformedSchoolData.objects.all().delete()
        data = SchoolData.objects.filter(school_name='[Statewide]')
        transformed_data = []

        for entry in data:
            transformed_entry = TransformedSchoolData(
                year=entry.school_year[:4],
                year_range=entry.school_year,
                place='WI',
                group_by=entry.group_by,
                group_by_value=entry.group_by_value,
                student_count=entry.student_count,
            )
            transformed_data.append(transformed_entry)

        TransformedSchoolData.objects.bulk_create(transformed_data)
        messages.success(self.request, f"Statewide transformation completed successfully. {len(transformed_data)} records were transformed.")
        return True

    def transform_tri_county(self):
        """ Transform 'Tri-County' data from the SchoolData model """
        if not SchoolData.objects.exists():
            messages.error(self.request, "No data available in the database. Please upload a file before running the transformation.")
            return False  # Indicate failure

        # Clear existing data in TransformedSchoolData to avoid duplicates
        TransformedSchoolData.objects.filter(place='Tri-County').delete()
        data = SchoolData.objects.filter(
            county__in=['Outagamie', 'Winnebago', 'Calumet'],  # Filter counties
            school_name__iexact='[Districtwide]'              # Filter school_name
        )
        transformed_data = []

        for entry in data:
            transformed_entry = TransformedSchoolData(
                year=entry.school_year[:4],
                year_range=entry.school_year,
                place='Tri-County',
                group_by=entry.group_by,
                group_by_value=entry.group_by_value,
                student_count=entry.student_count,
            )
            transformed_data.append(transformed_entry)

        TransformedSchoolData.objects.bulk_create(transformed_data)
        messages.success(self.request, f"Tri-County transformation completed successfully. {len(transformed_data)} records were transformed.")
        return True

    def apply_transformation(self, transformation_type):
        """ Apply the selected transformation type """
        if transformation_type == 'Statewide V01':
            return self.transform_statewide()
        elif transformation_type == 'Tri-County':
            return self.apply_tri_county_layer_transformation()
        else:
            messages.error(self.request, 'Unknown transformation type.')
            return False

    def apply_tri_county_layer_transformation(self):
        """ Apply Tri-County Layer Transformation """
        try:
            logger.info("Starting Tri-County Layer Transformation...")

            # Fetch filtered school data, including 'Unknown' county and school_name
            school_data = SchoolData.objects.filter(
                county__in=['Outagamie', 'Winnebago', 'Calumet', 'Unknown'],
                school_name__in=['[Districtwide]', 'Unknown']
            )
            logger.info(f"Filtered school data count: {school_data.count()}")

            grouped_data, group_by_sums, all_students_data, original_unknowns, aggregated_data = {}, {}, {}, {}, {}

            for record in school_data:
                period = f"{record.school_year.split('-')[0]}-20{record.school_year.split('-')[1]}" if '-' in record.school_year else record.school_year
                strat_label = record.stratification.label_name if record.stratification else "Unknown"
                group_by, group_by_value = record.group_by, record.group_by_value
                
                # Convert student_count to integer if it is a digit, else default to 0
                total_value = int(float(record.student_count)) if record.student_count.replace('.', '', 1).isdigit() else 0
                
                # Group by stratification, period, group_by, and group_by_value    
                strat_key = (strat_label, period, group_by, group_by_value)
                
                # Store the sum of group_by values for each group_by
                group_by_sums[group_by] = group_by_sums.get(group_by, 0) + total_value
                
                # Store the 'All Students' data for later calculation
                if group_by == "All Students" and group_by_value == "All Students":
                    all_students_data[(strat_label, period)] = total_value
                
                # Store the original 'Unknown' values for later calculation
                if group_by_value == "Unknown":
                    original_unknowns[(strat_label, period, group_by)] = total_value

                # Group and aggregate by strat_label, period, group_by, and group_by_value
                grouped_data.setdefault(strat_key, {
                    "layer": "Region",
                    "geoid": "fox-valley", 
                    "topic": "FVDEYLCV",
                    "period": period, 
                    "value": 0, 
                    "stratification": strat_label
                })["value"] += total_value

            # Calculate the 'Unknown' values for each group_by
            for (strat_label, period, group_by, group_by_value), data in grouped_data.items():
                if group_by_value == "Unknown":
                    known_total = group_by_sums.get(group_by, 0)
                    max_group_total = max(group_by_sums.values(), default=0)
                    data["value"] = max_group_total - known_total + original_unknowns.get((strat_label, period, group_by), 0)

            transformed_data = [MetopioTriCountyLayerTransformation(**{
                "layer": data["layer"],
                "geoid": data["geoid"],
                "topic": data["topic"],
                "stratification": data["stratification"],
                "period": data["period"],
                "value": data["value"]
            }) for data in grouped_data.values() if data["value"]!=0] # Exclude zero values during bulk insertion

            if transformed_data:
                with transaction.atomic():
                    MetopioTriCountyLayerTransformation.objects.all().delete()
                    MetopioTriCountyLayerTransformation.objects.bulk_create(transformed_data)
                logger.info(f"Successfully transformed {len(transformed_data)} records.")
            else:
                logger.info("No transformed data to insert.")

            return True

        except Exception as e:
            logger.error(f"Error during Tri-County Layer Transformation: {e}")
            return False

# Apply the county Layer Transformation 

    def apply_county_layer_transformation(self):
        try:
            # Reinitialize school Data
            logger.info("Starting County Layer Transformation...")

            # Fetch County GEOID entries
            county_geoid_entries = CountyGEOID.objects.filter(layer='County')
            county_geoid_map = {entry.name.split(" County, WI")[0].strip(): entry for entry in county_geoid_entries}

            logger.info(f"County GEOID entries count: {len(county_geoid_map)}")

            # STEP 2: Refresh dataset to include "Unknown" records
            school_data = SchoolData.objects.filter(
                models.Q(school_name="[Districtwide]") &
                models.Q(county__in=county_geoid_map.keys())
            )
            logger.info(f"Refetched school data count: {school_data.count()}")

            # STEP 3: Group Data
            grouped_data, group_by_sums, original_unknowns = {}, {}, {}

            for record in school_data:
                period = f"{record.school_year.split('-')[0]}-20{record.school_year.split('-')[1]}" if "-" in record.school_year else record.school_year
                strat_label = record.stratification.label_name 
                group_by, group_by_value = record.group_by, record.group_by_value
                geoid = county_geoid_map.get(record.county).geoid if county_geoid_map.get(record.county) else "Error"

                if geoid == "Error":
                    logger.warning(f"County GEOID not found for county: {record.county}")

                total_value = int(record.student_count) if record.student_count.isdigit() else 0
                strat_key = (strat_label, period, group_by, group_by_value, record.county)

                # Track group_by sums
                group_by_sums[group_by] = group_by_sums.get(group_by, 0) + total_value
                #logger.info(f"Group by sums: {group_by_sums}")
                # Store original 'Unknown' values for later calculation
                #logger.info(f"Group by value is {group_by_value}")
                if group_by_value == "Unknown":
                    
                    original_unknowns[(strat_label, period, group_by)] = total_value
                    logger.info(f"Original Unknowns: {original_unknowns}")
                    known_total = group_by_sums.get(group_by, 0)
                    max_group_total = max(group_by_sums.values(), default=0)
                    total_value = max_group_total - known_total + original_unknowns.get((strat_label, period, group_by), 0)

                # Add to grouped data
                grouped_data.setdefault(strat_key, {
                    "layer": "County",
                    "geoid": geoid,
                    "topic": "FVDEYLCV",
                    "stratification": strat_label,
                    "period": period,
                    "value": 0,
                })["value"] += total_value
            
        
            # Calculate the 'Unknown' values for each grouped data
            # for (strat_label, period, group_by, group_by_value,record.county), data in grouped_data.items():
                
            #     geoid = county_geoid_map.get(record.county).geoid if record.county in county_geoid_map else "Error"  
            #     if  group_by_value == "Unknown":
                    
            #         known_total = group_by_sums.get(group_by, 0)
                    
                    
            #         max_group_total = max(group_by_sums.values(), default=0)
                    
            #         data["value"] = max_group_total - known_total + original_unknowns.get((strat_label, period, group_by), 0)
            #     # #Assign "UNK7" or "UNK8" for all Gender/Grade Level unknowns
                # #Ensure "UNK7" or "UNK8" are only assigned if they exist in redacted_county_map
               
                #     if group_by == "Gender" :
                #         data["stratification"] = "UNK7" 
                #         #logger.info(f"{data["geoid"]} and county is {record.county}geoid is {geoid}")
                        
                #         #logger.info(f"Data Value at this point {data["value"]} and county is {record.county}geoid is {geoid}")
                #     elif group_by == "Grade Level" :
                #         data["stratification"] = "UNK8"
                        
                

            # STEP 4: Bulk Insert Transformed Data
            transformed_data = [
                CountyLayerTransformation(**{
                    "layer": data["layer"],
                    "geoid": data["geoid"],
                    "topic": data["topic"],
                    "stratification": data["stratification"],
                    "period": data["period"],
                    "value": data["value"]
                }) for data in grouped_data.values() if data["value"] != 0
            ]

            # Insert transformed data
            if transformed_data:
                with transaction.atomic():
                    CountyLayerTransformation.objects.all().delete()
                    CountyLayerTransformation.objects.bulk_create(transformed_data)
                logger.info(f"Successfully transformed {len(transformed_data)} records.")
            else:
                logger.info("No transformed data to insert.")

            return True

        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            line_number = tb[-1][1]
            logger.error(f"Error during County Layer Transformation: {e} at line number {line_number}")
            return False


    def transform_Metopio_StateWideLayer(self):
        """Apply StateWide Layer Transformation"""
        if not SchoolData.objects.exists():
            messages.error(self.request, 'No data found in the SchoolData model. Please upload a file first.')
            return False
        try:
            logger.info("Starting Metopio StateWide Layer Transformation...")
            # Clear the existing data in the MetopioStateWideLayerTransformation to avoid duplicates
            MetopioStateWideLayerTransformation.objects.all().delete()
            
            #Define filters for DISTRICT_NAME =[Statewide]
            district_name_filter = '[Statewide]'
            
            #Fetch filtered school data
            
            school_data = SchoolData.objects.filter(district_name=district_name_filter)
            logger.info(f"Filtered school data count: {school_data.count()}")
            
            #Group data by stratification and period
            grouped_data = {}
            for record in school_data:
                #Transform the period field
                school_year = record.school_year
                if '-' in school_year:
                    start_year, end_year = school_year.split('-')  # unpacks the tuple
                    period = f"{start_year}-20{end_year}"  # Transform to 2023-2024 format
                else:
                    period = school_year

                # Default to "Error" if stratification is None
                stratification = record.stratification.label_name if record.stratification else "Error"

                # Group by stratification and period
                strat_key = (stratification, period)
                if strat_key not in grouped_data:
                    grouped_data[strat_key] = {
                        "layer": "State",
                        "geoid": "WI",
                        "topic": "FVDEYLCV",
                        "stratification": stratification,
                        "period": period,
                        "value": int(record.student_count) if record.student_count.isdigit() else 0,
                    }
                else:
                    grouped_data[strat_key]["value"] += int(record.student_count) if record.student_count.isdigit() else 0
                    
            # Prepare transformed data for bulk insertion
            transformed_data = [
                MetopioStateWideLayerTransformation(
                    layer=data["layer"],
                    geoid=data["geoid"],
                    topic=data["topic"],
                    stratification=data["stratification"],
                    period=data["period"],
                    value=data["value"],
                )
                for data in grouped_data.values()
            ]
            
            # Insert transformed data in bulk
            with transaction.atomic():
                MetopioStateWideLayerTransformation.objects.all().delete()  # Clear existing data
                MetopioStateWideLayerTransformation.objects.bulk_create(transformed_data)
            logger.info(f"Successfully transformed {len(transformed_data)} records.")
            return True
        
        except Exception as e:
            logger.error(f"Error during Metopio StateWide Layer Transformation: {e}")
            return False
            

#Just need to extract the zip code but still I am using the generic splitter
# to make this code to be reuusable for other layers if needed
# Please not why am I using the entry instead of the entry.geoid
#By storing the entire CountyGEOID instance (entry) in the dictionary
# instead of just entry.geoid, you can access all fields of the instance later 
# (e.g., name, geoid, etc.). This provides more flexibility.
# county_geoid_map["Outagamie"].geoid  # Access the GEOID of Outagamie
# county_geoid_map["Outagamie"].name   # Access the name  of Outagamie
#The resulting dictionary will look like this:
# {
#         "Outagamie": <CountyGEOID instance for Outagamie>,
#         "Winnebago": <CountyGEOID instance for Winnebago>,
#         "Calumet": <CountyGEOID instance for Calumet>,
#         ...
#  }

    def transforms_Metopio_ZipCodeLayer(self):
        try:
            logger.info("Starting Metopio ZipCode Layer Transformation...")
                   
            # Remaining transformation logic

            # Fetch and filter County GEOID entries
            county_geoid_entries = CountyGEOID.objects.filter(layer="Zip code")
            logger.info(f"Filtered County GEOID entries count: {county_geoid_entries.count()}")

            # Create a map to store the Zip Code and its corresponding GEOID from the CountyGEOID entries
            zip_code_geoid_map = {entry.name: entry.geoid for entry in county_geoid_entries}
            logger.info(f"Zip Code GEOID map: {zip_code_geoid_map}")

            # Fetch and join SchoolData with SchoolAddressFile dynamically
            school_data = (
                SchoolData.objects.filter(
                    ~Q(county__startswith ='['),                        # Exclude records with county names in square brackets
                    county__in=['Outagamie', 'Winnebago', 'Calumet']  # Filter for specified counties

                )
                .prefetch_related('address_details')  # Fetch related address details in a single query
                .distinct()  # Ensure unique records
            )
            logger.info(f"Filtered school data count: {school_data.count()}")

            # Process records for transformation
            grouped_data = {}
            report_data = []  # Collect reporting data here

            for record in school_data:
                # Iterate over the related address_details objects
                
                for address_details in record.address_details.all():
                   
                    # Extract the zip code
                    zip_code = address_details.zip_code

                    # Map the zip code to its GEOID
                    geoid = zip_code_geoid_map.get(zip_code, "Error")
                    if geoid == "Error":
                        logger.warning(f"GEOID not found for zip code: {zip_code}")
                        continue

                    # Transform the period field
                    school_year = record.school_year
                    if '-' in school_year:
                        start_year, end_year = school_year.split('-')
                        period = f"{start_year}-20{end_year}"  # Transform to 2023-2024 format
                    else:
                        period = school_year

                    # Default to "Error" if stratification is None
                    stratification = record.stratification.label_name if record.stratification else "Error"

                    # Group by stratification and period
                    strat_key = (stratification, geoid)

                    if strat_key not in grouped_data:
                        grouped_data[strat_key] = {
                            "layer": "Zip code",
                            "geoid": geoid,
                            "topic": "FVDEYLCV",
                            "stratification": stratification,
                            "period": period,
                            "value": int(record.student_count) if record.student_count.isdigit() else 0,
                        }
                    else:
                        grouped_data[strat_key]["value"] += int(record.student_count) if record.student_count.isdigit() else 0

                    # Add to reporting data
                    report_data.append({
                        "school_year": record.school_year,
                        "district_name": record.district_name,
                        "school_name": record.school_name,
                        "county": record.county,
                        "zip_code": zip_code,
                        "geoid": geoid,
                        "stratification": stratification,
                        "student_count": record.student_count,
                    })

            # Prepare transformed data for bulk insertion
            transformed_data = [
                ZipCodeLayerTransformation(
                    layer=data["layer"],
                    geoid=data["geoid"],
                    topic=data["topic"],
                    stratification=data["stratification"],
                    period=data["period"],
                    value=data["value"],
                )
                for data in grouped_data.values()
            ]

            # Insert transformed data in bulk
            with transaction.atomic():
                ZipCodeLayerTransformation.objects.all().delete()  # Clear existing data
                ZipCodeLayerTransformation.objects.bulk_create(transformed_data)

            logger.info(f"Successfully transformed {len(transformed_data)} records.")

            # Output the reporting data to a file or database
            logger.info(f"Generated report with {len(report_data)} entries.")
            
            return True

        except Exception as e:
            logger.error(f"Error during Metopio ZipCode Layer Transformation: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False


# data_processor/transformers.py
    def transform_Metopio_CityLayer(self):
        try:
            logger.info("Starting Metopio City Layer Transformation...")
                   
            # Remaining transformation logic

            # Fetch and filter County GEOID entries
            county_geoid_entries = CountyGEOID.objects.filter(layer="City or town")
            logger.info(f"Filtered County GEOID entries count: {county_geoid_entries.count()}")

            # Create a map to store the City and its corresponding GEOID from the County GEOID entries
            city_geoid_map = {entry.name: entry.geoid for entry in county_geoid_entries}
            logger.info(f"City GEOID map: {city_geoid_map}")

            # Fetch and join SchoolData with SchoolAddressFile dynamically
            school_data = (
                SchoolData.objects.filter(
                    ~Q(county__startswith ='['),                        # Exclude records with county names in square brackets
                    county__in=['Outagamie', 'Winnebago', 'Calumet']  # Filter for specified counties

                )
                .prefetch_related('address_details')  # Fetch related address details in a single query
                .distinct()  # Ensure unique records
            )
            logger.info(f"Filtered school data count: {school_data.count()}")

            # Process records for transformation
            grouped_data = {}
            report_data = []  # Collect reporting data here

            for record in school_data:
                # Iterate over the related address_details objects
                
                for address_details in record.address_details.all():
                   
                    # Extract the city
                    city = address_details.city + ", WI"   # Adding ", WI" to match the format in the CountyGEOID file

                    # Map the city from the SchoolAddressFile  to its GEOID from the CountyGEOID file
                    geoid = city_geoid_map.get(city, "Error")
                    if geoid == "Error":
                        logger.warning(f"GEOID not found for city: {city}")
                        continue

                    # Transform the period field
                    school_year = record.school_year
                    if '-' in school_year:
                        start_year, end_year = school_year.split('-')
                        period = f"{start_year}-20{end_year}"  # Transform to 2023-2024 format
                    else:
                        period = school_year

                    # Default to "Error" if stratification is None
                    stratification = record.stratification.label_name if record.stratification else "Error"

                    # Group by stratification and period
                    strat_key = (stratification, geoid)

                    if strat_key not in grouped_data:
                        grouped_data[strat_key] = {
                            "layer": "City or town",
                            "geoid": geoid,
                            "topic": "FVDEYLCV",
                            "stratification": stratification,
                            "period": period,
                            "value": int(record.student_count) if record.student_count.isdigit() else 0,
                        }
                    else:
                        grouped_data[strat_key]["value"] += int(record.student_count) if record.student_count.isdigit() else 0
                        
                    # Add to reporting data
                    report_data.append({
                        "school_year": record.school_year,
                        "district_name": record.district_name,
                        "school_name": record.school_name,
                        "county": record.county,
                        "city": city,
                        "geoid": geoid,
                        "stratification": stratification,
                        "student_count": record.student_count,
                    })
            # Prepare transformed data for bulk insertion
            transformed_data = [
                MetopioCityLayerTransformation(
                    layer=data["layer"],
                    geoid=data["geoid"],
                    topic=data["topic"],
                    stratification=data["stratification"],
                    period=data["period"],
                    value=data["value"],
                )
                for data in grouped_data.values()
            ]

            # Insert transformed data in bulk
            
            with transaction.atomic():
                MetopioCityLayerTransformation.objects.all().delete() # Clear existing data
                MetopioCityLayerTransformation.objects.bulk_create(transformed_data)
            logger.info(f"Successfully transformed {len(transformed_data)} records.")
            
            # Output the reporting data to a file or database
            logger.info(f"Generated report with {len(report_data)} entries.")
            
            return True
        except Exception as e:
            logger.error(f"Error during Metopio City Layer Transformation: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False