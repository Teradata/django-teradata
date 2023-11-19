import datetime
import pytz

from django.core.exceptions import ImproperlyConfigured
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.base.base import NO_DB_ALIAS
from django.db.backends.base.client import BaseDatabaseClient as DatabaseClient
from django.utils.asyncio import async_unsafe
from django.utils.regex_helper import _lazy_re_compile

try:
    import teradatasql as Database
except ImportError as e:
    raise ImproperlyConfigured("Error loading teradatasql connector module: %s" % e)

# Some of these import teradata connector, so import them after checking if it's installed.
from . import __version__                                   # NOQA isort:skip
from .creation import DatabaseCreation                      # NOQA isort:skip
from .features import DatabaseFeatures                      # NOQA isort:skip
from .introspection import DatabaseIntrospection            # NOQA isort:skip
from .operations import DatabaseOperations                  # NOQA isort:skip
from .schema import DatabaseSchemaEditor                    # NOQA isort:skip
from .utils import get_timezone_offset                      # NOQA isort:skip


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'teradata'
    display_name = 'Teradata'
    # This is a readonly adapter, no need for sql schema information
    data_types = {
        'AutoField': 'INTEGER',
        'BigAutoField': 'BIGINT',
        'BooleanField': 'BYTEINT',
        'CharField': 'CHARACTER',
        'DateField': 'DATE',
        'DateTimeField': 'TIMESTAMP WITH TIME ZONE',
        'DecimalField': 'DECIMAL(%(max_digits)s,%(decimal_places)s)',
        'DurationField': 'NUMBER(38,0)',
        'FileField': 'VARCHAR(%(max_length)s)',
        'FilePathField': 'VARCHAR(%(max_length)s)',
        'FloatField': 'FLOAT',
        'IntegerField': 'INTEGER',
        'BigIntegerField': 'BIGINT',
        'GenericIPAddressField': 'VARCHAR(39)',
        'NullBooleanField': 'BYTEINT',
        'PositiveBigIntegerField': 'BIGINT',
        'PositiveIntegerField': 'INTEGER',
        'PositiveSmallIntegerField': 'SMALLINT',
        'SlugField': 'VARCHAR(%(max_length)s)',
        'SmallAutoField': 'SMALLINT',
        'SmallIntegerField': 'SMALLINT',
        'TextField': 'VARCHAR',
        'TimeField': 'TIME',
        'UUIDField': 'VARCHAR(32)',
    }
    data_types_suffix = {}
    operators = {
        'exact': '= %s',
        'iexact': "LIKE %s (NOT CASESPECIFIC) ESCAPE '\\'",
        'contains': "LIKE %s ESCAPE '\\'",
        'icontains': "LIKE %s (NOT CASESPECIFIC) ESCAPE '\\'",
        'gt': '> %s',
        'gte': '>= %s',
        'lt': '< %s',
        'lte': '<= %s',
        'startswith': "LIKE %s ESCAPE '\\'",
        'endswith': "LIKE %s ESCAPE '\\'",
        'istartswith': "LIKE %s (NOT CASESPECIFIC) ESCAPE '\\'",
        'iendswith': "LIKE %s (NOT CASESPECIFIC) ESCAPE '\\'",
    }
    pattern_esc = r"OREPLACE(OREPLACE(OREPLACE({}, '\', '\\'), '%%', '\%%'), '_', '\_')"
    pattern_ops = {
        'contains': "LIKE '%%' || {} || '%%' ESCAPE '\\'",
        'icontains': "LIKE '%%' || {} || '%%' (NOT CASESPECIFIC) ESCAPE '\\'",
        'startswith': "LIKE {} || '%%' ESCAPE '\\'",
        'istartswith': "LIKE {} || '%%' (NOT CASESPECIFIC) ESCAPE '\\'",
        'endswith': "LIKE '%%' || {} ESCAPE '\\'",
        'iendswith': "LIKE '%%' || {} (NOT CASESPECIFIC) ESCAPE '\\'",
    }

    Database = Database
    SchemaEditorClass = DatabaseSchemaEditor

    # Classes instantiated in __init__().
    client_class = DatabaseClient
    creation_class = DatabaseCreation
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations

    settings_is_missing = "settings.DATABASES is missing '%s' for 'django_teradata'."

    def get_connection_params(self):
        settings_dict = self.settings_dict
        conn_params = {
            'logmech': 'TD2',
            'tmode': 'ANSI'
        }

        if settings_dict.get('NAME'):
            conn_params['database'] = settings_dict['NAME']
        # check whether this is a test run
        elif self.alias == NO_DB_ALIAS:
            conn_params['database'] = 'DBC'
        else:
            raise ImproperlyConfigured(self.settings_is_missing % 'NAME')

        if settings_dict['HOST']:
            conn_params['host'] = settings_dict['HOST']

        if settings_dict['USER']:
            conn_params['user'] = settings_dict['USER']
        else:
            raise ImproperlyConfigured(self.settings_is_missing % 'USER')

        if settings_dict['PASSWORD']:
            conn_params['password'] = settings_dict['PASSWORD']
        else:
            raise ImproperlyConfigured(self.settings_is_missing % 'PASSWORD')

        return conn_params

    @async_unsafe
    def get_new_connection(self, conn_params):
        return Database.connect(**conn_params)

    def ensure_timezone(self):
        if self.connection is None:
            return False

        tz_offset = get_timezone_offset(self.timezone_name)
        with self.connection.cursor() as cursor:
            query = "HELP SESSION"
            session_tz_offset = cursor.execute(query).fetchone()[9]
        if tz_offset != session_tz_offset:
            with self.connection.cursor() as cursor:
                cursor.execute(f"SET TIME ZONE '{tz_offset}'")
            return True
        return False

    def init_connection_state(self):
        timezone_changed = self.ensure_timezone()
        if timezone_changed:
            # Commit after setting the time zone
            # (This is copied from the postgresql backend.)
            if not self.get_autocommit():
                self.connection.commit()

    @async_unsafe
    def create_cursor(self, name=None):
        return TeradataCursorWrapper(self.connection)

    def _set_autocommit(self, autocommit):
        with self.wrap_database_errors:
            self.connection.autocommit = autocommit

    def is_usable(self):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute('SELECT now()')
        except Database.Error:
            return False
        else:
            return True


FORMAT_QMARK_REGEX = _lazy_re_compile(r"(?<!%)%s")


class TeradataCursorWrapper(Database.TeradataCursor):
    """
      Django uses "format" style placeholders, but Teradata uses "qmark" style.
      This fixes it -- but note that if you want to use a literal "%s" in a query,
      you'll need to use "%%s".
      """

    def execute(self, query, params=None, ignore_errors=None):
        if params is None:
            return super().execute(query, None, ignore_errors)
        query = self.convert_query(query)
        return super().execute(query, params, ignore_errors)

    def executemany(self, query, param_list, ignore_errors=None):
        query = self.convert_query(query)
        return super().executemany(query, param_list, ignore_errors)

    def convert_query(self, query):
        return FORMAT_QMARK_REGEX.sub("?", query).replace("%%", "%")
