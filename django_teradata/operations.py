from django.conf import settings
from django.db.backends.base.operations import BaseDatabaseOperations

from .utils import get_timezone_offset            # NOQA isort:skip


class DatabaseOperations(BaseDatabaseOperations):

    compiler_module = 'django_teradata.compiler'
    explain_prefix = 'EXPLAIN'

    def limit_offset_sql(self, low_mark, high_mark):
        # Teradata does not support LIMIT and OFFSET syntax
        # replace 'LIMIT %s' with "TOP %s" .
        limit, offset = self._get_limit_offset_params(low_mark, high_mark)
        if offset:
            raise NotImplementedError("Teradata does not support OFFSET syntax.")

        return ('TOP %s' % limit) if limit else None

    def no_limit_value(self):
        return None

    def quote_name(self, name):
        if name.startswith('"') and name.endswith('"'):
            return name  # Quoting once is enough.
        return '"%s"' % name.upper().replace('.', '"."')

    def sql_flush(self, style, tables, *, reset_sequences=False, allow_cascade=False):
        # this is a readonly adapter
        return []

    def adapt_datefield_value(self, value):
        return value

    def adapt_datetimefield_value(self, value):
        return value

    def adapt_timefield_value(self, value):
        return value

    def _convert_sql_to_tz(self, sql, params, tzname):
        if tzname and settings.USE_TZ:
            tz_offset = get_timezone_offset(tzname)
            return f"{sql} AT TIME ZONE {tz_offset}", params
        return sql, params

    def date_extract_sql(self, lookup_type, sql, params):
        lookup = lookup_type.upper()
        if lookup in ["YEAR", "MONTH", "DAY"]:
            return f"EXTRACT({lookup_type} FROM {sql})", params

        raise ValueError(f"Invalid lookup type: {lookup_type!r}")

    def datetime_cast_date_sql(self, sql, params, tzname):
        return self._convert_sql_to_tz(f"CAST({sql} AS DATE)", params, tzname)

    def datetime_cast_time_sql(self, sql, params, tzname):
        return self._convert_sql_to_tz(f"CAST({sql} AS TIME)", params, tzname)

    def datetime_extract_sql(self, lookup_type, sql, params, tzname):
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        lookup = str.upper(lookup_type)

        if lookup in ["YEAR", "MONTH", "DAY", "MINUTE", "SECOND"]:
            return f"EXTRACT({lookup} FROM {sql})", params

        raise ValueError(f"Invalid lookup type: {lookup_type!r}")

    def time_extract_sql(self, lookup_type, sql, params):
        lookup = str.upper(lookup_type)

        if lookup in ["HOUR", "MINUTE", "SECOND"]:
            return f"EXTRACT({lookup} FROM {sql})", params

        raise ValueError(f"Invalid lookup type: {lookup_type!r}")

