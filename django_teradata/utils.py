import datetime
import pytz
import django
from django.core.exceptions import ImproperlyConfigured
from django.utils.version import get_version_tuple


def check_django_compatibility():
    """
    Verify that this version of django-teradata is compatible with the
    installed version of Django. For example, any django-teradata 4.1.x is
    compatible with Django 4.1.y.
    """
    from . import __version__
    if django.VERSION[:2] != get_version_tuple(__version__)[:2]:
        raise ImproperlyConfigured(
            'You must use the latest version of django-teradata {A}.{B}.x '
            'with Django {A}.{B}.y (found django-teradata {C}).'.format(
                A=django.VERSION[0],
                B=django.VERSION[1],
                C=__version__,
            )
        )


def get_timezone_offset(tzname):
    timezone_info = datetime.datetime.now(pytz.timezone(tzname)).strftime('%z')
    return f"{'-' if timezone_info[0] == '-' else ''}{timezone_info[1:3]}:{timezone_info[3:]}"
