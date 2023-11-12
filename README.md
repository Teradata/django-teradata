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
* django inspectdb does not identify the primary keys for the models. The keys need
  to be manually defined for the models.

## Troubleshooting

