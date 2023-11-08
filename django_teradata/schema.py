from django.db.backends.base.schema import BaseDatabaseSchemaEditor


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):
    def _constraint_names(self, model, column_names=None, unique=None, primary_key=None, index=None, foreign_key=None,
                          check=None, type_=None, exclude=None):
        raise NotImplementedError("Teradata backend adapter is a read only adapter.")

    def execute(self, sql, params=()):
        raise NotImplementedError("Teradata backend adapter is a read only adapter.")

    def quote_value(self, value):
        raise NotImplementedError("Teradata backend adapter is a read only adapter.")

    def prepare_default(self, value):
        raise NotImplementedError("Teradata backend adapter is a read only adapter.")
