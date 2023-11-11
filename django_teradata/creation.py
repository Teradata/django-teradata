from django.db.backends.base.creation import BaseDatabaseCreation


class DatabaseCreation(BaseDatabaseCreation):
    def _quote_name(self, name):
        return self.connection.ops.quote_name(name)

    def _database_exists(self, cursor, database_name):
        try:
            cursor.execute(f'DATABASE {database_name}')
        except Exception as exc:
            if 'does not exist.' in str(exc):
                return False
            raise
        return True

    def _execute_create_test_db(self, cursor, parameters, keepdb=False):
        if not keepdb or not self._database_exists(cursor, parameters['dbname']):
            # Try to create a database if keepdb=False or if keepdb=True and
            # the database doesn't exist.
            super()._execute_create_test_db(cursor, parameters, keepdb)
