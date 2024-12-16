# data_processor/transformers.py

from .models import SchoolData, TransformedSchoolData
from django.contrib import messages

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
