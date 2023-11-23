from django.db.backends.base.schema import BaseDatabaseSchemaEditor


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):
    
    sql_create_fk = (
        "ALTER TABLE %(table)s ADD CONSTRAINT %(name)s FOREIGN KEY (%(column)s) "
        "REFERENCES WITH NO CHECK OPTION %(to_table)s (%(to_column)s)%(deferrable)s"
    )
    
    sql_create_index = (
        "CREATE INDEX %(name)s (%(columns)s) ON %(table)s "
        "%(include)s%(extra)s%(condition)s"
    )
    
    sql_create_unique_index = (
        "CREATE UNIQUE INDEX %(name)s (%(columns)s) ON %(table)s "
        "%(include)s%(condition)s"
    )
    
    def execute(self, sql, params=()):
        super().execute(sql, params)
