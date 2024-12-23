from django import forms

class UploadFileForm(forms.Form):
    file = forms.FileField(label="Main File", required=True)
    stratifications_file = forms.FileField(label="Stratifications File (Optional)", required=False)
    county_geoid_file = forms.FileField(label="County GEOID File (Optional)", required=False)
    school_address_file = forms.FileField(label="School Address File (Optional)", required=False)