from django.db import models

class Stratification(models.Model):
    group_by = models.CharField(max_length=100, default="Default Group")
    group_by_value = models.CharField(max_length=200, default="Default Group")
    label_name = models.CharField(max_length=200, default="Default Group")

    def __str__(self):
        return f"{self.group_by} - {self.group_by_value}"
    

class CountyGEOID(models.Model):
    layer = models.CharField(max_length=50)
    name = models.CharField(max_length=100)
    geoid = models.CharField(max_length=50, unique=True)
    
class SchoolAddressFile(models.Model):
    lea_code = models.CharField(max_length=10, verbose_name="LEA Code")
    district_name = models.CharField(max_length=255, verbose_name="District Name")
    school_code = models.CharField(max_length=10, verbose_name="School Code")
    school_name = models.CharField(max_length=255, verbose_name="School Name")
    organization_type = models.CharField(max_length=100, verbose_name="Organization Type")
    school_type = models.CharField(max_length=100, verbose_name="School Type")
    low_grade = models.CharField(max_length=10, verbose_name="Low Grade")
    high_grade = models.CharField(max_length=10, verbose_name="High Grade")
    address = models.CharField(max_length=255, verbose_name="Address")
    city = models.CharField(max_length=100, verbose_name="City")
    state = models.CharField(max_length=2, verbose_name="State")
    zip_code = models.CharField(max_length=10, verbose_name="Zip")
    cesa = models.CharField(max_length=10, verbose_name="CESA")
    locale = models.CharField(max_length=100, verbose_name="Locale")
    county = models.CharField(max_length=100, verbose_name="County")
    current_status = models.CharField(max_length=50, verbose_name="Current Status")
    categories_and_programs = models.TextField(null=True, blank=True, verbose_name="Categories And Programs")
    virtual_school = models.CharField(max_length=50, null=True, blank=True, verbose_name="Virtual School")
    ib_program = models.CharField(max_length=50, null=True, blank=True, verbose_name="IB Program")
    phone_number = models.CharField(max_length=20, verbose_name="Phone Number")
    fax_number = models.CharField(max_length=20, null=True, blank=True, verbose_name="Fax Number")
    charter_status = models.BooleanField(verbose_name="Charter Status")
    website_url = models.URLField(max_length=255, null=True, blank=True, verbose_name="Website URL")

    def __str__(self):
        return f"{self.school_name} ({self.district_name})"
    

    def __str__(self):
        return f"{self.layer} - {self.name} - {self.geoid}"
# Create your models here.
class SchoolData(models.Model):
    school_year = models.CharField(max_length=7)
    agency_type = models.CharField(max_length=50)
    cesa = models.CharField(max_length=10)
    county = models.CharField(max_length=50)
    district_code = models.CharField(max_length=10)
    school_code = models.CharField(max_length=10)
    grade_group = models.CharField(max_length=50)
    charter_ind = models.CharField(max_length=4)
    district_name = models.CharField(max_length=100)
    school_name = models.CharField(max_length=100)
    group_by = models.CharField(max_length=50)
    group_by_value = models.CharField(max_length=200)
    student_count = models.CharField(max_length=20)
    percent_of_group = models.CharField(max_length=20)
    place = models.CharField(max_length=100, null=True, blank=True)
    stratification = models.ForeignKey(Stratification, on_delete=models.SET_NULL, null=True, blank=True)
    geoid = models.ForeignKey(CountyGEOID, on_delete=models.SET_NULL, null=True, blank=True)
    


class TransformedSchoolData(models.Model):
    year = models.CharField(max_length=7)
    year_range = models.CharField(max_length=50)
    place = models.CharField(max_length=100, null=True, blank=True)
    group_by = models.CharField(max_length=50)
    group_by_value = models.CharField(max_length=200)
    student_count = models.CharField(max_length=20)

    class Meta:
        ordering = ['year']  # Default ordering by 'year' field

class MetopioStateWideLayerTransformation(models.Model):
    layer = models.CharField(max_length=50, default='State')  # Constant value: 'State'
    geoid = models.CharField(max_length=50, default='WI')  # Constant value: 'wisconsin'
    topic = models.CharField(max_length=50, default='FVDEYLCV')  # Constant value: 'FVDEYLCV'
    stratification = models.TextField(blank=True)  # To store stratification notes
    period = models.CharField(max_length=20)  # Transformed SCHOOL_YEAR (e.g., 2023-24 → 2023-2024)
    value = models.PositiveIntegerField()  # Derived from STUDENT_COUNT

    class Meta:
        verbose_name = 'Metopio Statewide Data Transformation'
        verbose_name_plural = 'Metopio Statewide Data Transformations'
        ordering = ['period', 'stratification']  # Add this line

class MetopioTriCountyLayerTransformation(models.Model):
    layer = models.CharField(max_length=50, default='Region')  # Constant value: 'Region'
    geoid = models.CharField(max_length=50, default='fox-valley')  # Constant value: 'fox-valley'
    topic = models.CharField(max_length=50, default='FVDEYLCV')  # Constant value: 'FVDEYLCV'
    stratification = models.TextField(blank=True)  # To store stratification notes
    period = models.CharField(max_length=20)  # Transformed SCHOOL_YEAR (e.g., 2023-24 → 2023-2024)
    value = models.PositiveIntegerField()  # Derived from STUDENT_COUNT

    class Meta:
        verbose_name = 'Metopio Data Transformation'
        verbose_name_plural = 'Metopio Data Transformations'
        ordering = ['period', 'stratification']  # Add this line




class CountyLayerTransformation(models.Model):
    layer = models.CharField(max_length=50, default='County')
    geoid = models.CharField(max_length=50)  # Change this to CharField
    topic = models.CharField(max_length=50, default='FVDEYLCV')
    stratification = models.TextField(blank=True)
    period = models.CharField(max_length=20)
    value = models.PositiveIntegerField()

    class Meta:
        verbose_name = 'County Layer Transformation'
        verbose_name_plural = 'County Layer Transformations'
        ordering = ['period', 'stratification']
        
class ZipCodeLayerTransformation(models.Model):
    layer = models.CharField(max_length=50, default='County')
    geoid = models.CharField(max_length=50)  # Change this to CharField
    topic = models.CharField(max_length=50, default='FVDEYLCV')
    stratification = models.TextField(blank=True)
    period = models.CharField(max_length=20)
    value = models.PositiveIntegerField()

    class Meta:
        verbose_name = 'County Layer Transformation'
        verbose_name_plural = 'County Layer Transformations'
        ordering = ['period', 'stratification']