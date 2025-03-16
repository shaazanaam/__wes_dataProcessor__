# DataProcessorWES

DataProcessorWES is a Django application designed to process and transform school data. The application supports multiple transformation types and provides an interface for uploading data files and viewing transformed data.

## Features

- Upload and process main data files and optional stratification files.
- Supports multiple transformation types including Tri-County, County-Layer, Metopio Statewide, Zipcode, and City-Town.
- Provides views to display transformed data with pagination.
- Allows downloading transformed data in Excel and CSV formats.

## Requirements

- Python 3.8+
- Django 3.2+
- PostgreSQL (or any other supported database)
- Heroku CLI (for deployment)

## Setup

### 1. Clone the Repository

```sh
git clone https://github.com/your-username/DataProcessorWES.git
cd DataProcessorWES
python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`
pip install -r requirements.txt
SECRET_KEY=your-secret-key
DEBUG=True
ALLOWED_HOSTS=localhost,127.0.0.1
DATABASE_URL=your-database-url

Apply Migrations

python manage.py migrate


Create a super user

python manage.py createsuperuser


RUN THE SERVER

python manage.py runserver



YOU CAN THE DEPLOY TO HEROKU OR AWS or whatever platoform you like