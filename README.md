# Experimental readonly Teradata backend for Django 4.2.x

## Usage


Configure the Django `DATABASES` setting similar to this:

```python
DATABASES = {
    'default': {
        'ENGINE': 'django_teradata',
        'NAME': 'MY_DATABASE',
        'HOST': 'hostname[:port]',
        'USER': 'my_user',
        'PASSWORD': 'my_password'
    }
}
```

The backend enforces ANSI mode and TD2 logmech. 

## Notes on Django fields

## Known issues and limitations

This list is not exhaustive:
* django inspectdb does not identify the primary keys or foreign keys for the models. 
These keys need to be manually defined in the models.
* DATE_TRUNC function is not implemented  

## Troubleshooting

## Testing
Follow these steps:
* clone djago repo locally (The local copy folder path will be reffered to as DJANGO_DIR)
* checkout specifid django version: `cd $DJANGO_Dir; git checkout 4.2.7`
* install python requirements: `pip install -r $DJANGO_Dir/tests/requirements/py3.txt`
* create teradata_setttings.py in $DJANGO_Dir/tests with similar config:
```python
DATABASES = {
    'default': {
        'ENGINE': 'django_teradata',
        'NAME': 'MY_DATABASE',
        'HOST': 'hostname[:port]',
        'USER': 'my_user',
        'PASSWORD': 'my_password'
        "TEST": {
            "NAME": "django_tests_db"
        }
    },
}

LANGUAGE_CODE = "en-us"

TIME_ZONE = "UTC"

USE_I18N = True

USE_TZ = False

DEFAULT_AUTO_FIELD = 'django.db.models.AutoField'
SECRET_KEY = 'django_tests_secret_key'

```
* merge your changes with django-teradata/test_only_data_update branch. This branch allows for CRUD operations.
* run the tests: `$DJANGO_DIR/tests/runtests.py --settings teradata_settings -v 2 aggregation`

