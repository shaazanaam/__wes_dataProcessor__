# data_processor/transformers.py

from .models import SchoolData, TransformedSchoolData,MetopioDataTransformation
from django.db import transaction
import logging

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
        if transformation_type == 'Statewide':
            return self.transform_statewide()
        elif transformation_type == 'Tri-County':
            return self.transform_tri_county()
        else:
            messages.error(self.request, 'Unknown transformation type.')
            return False

    def apply_tri_county_layer_transformation(self):
        try:
            logger.info("Starting Tri-County Layer Transformation...")

            # Define filters for COUNTY and SCHOOL_NAME
            county_filter = ['Outagamie', 'Winnebago', 'Calumet']
            school_name_filter = '[Districtwide]'

            # Fetch filtered school data
            school_data = SchoolData.objects.filter(
                county__in=county_filter,
                school_name=school_name_filter
            )

            logger.info(f"Filtered school data count: {school_data.count()}")

            # Group data by stratification and period
            grouped_data = {}
            for record in school_data:
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
                strat_key = (stratification, period)
                if strat_key not in grouped_data:
                    grouped_data[strat_key] = {
                        "layer": "Region",
                        "geoid": "fox-valley",
                        "topic": "FVDEYLCV",
                        "stratification": stratification,
                        "period": period,
                        "value": int(record.student_count) if record.student_count.isdigit() else 0,
                    }
                else:
                    grouped_data[strat_key]["value"] += int(record.student_count) if record.student_count.isdigit() else 0

            # Prepare transformed data for bulk insertion
            transformed_data = [
                MetopioDataTransformation(
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
                MetopioDataTransformation.objects.all().delete()  # Clear existing data
                MetopioDataTransformation.objects.bulk_create(transformed_data)

            logger.info(f"Successfully transformed {len(transformed_data)} records.")
            return True

        except Exception as e:
            logger.error(f"Error during Tri-County Layer Transformation: {e}")
            return False

