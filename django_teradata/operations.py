from django.db.backends.base.operations import BaseDatabaseOperations


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
