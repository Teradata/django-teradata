from django.db.models.expressions import Exists


def override_exists_as_sql(self, compiler, connection):
    # avoid "[Error 6916] TOP N Syntax error: Top N option is not supported in subquery." in EXITS clauses
    # by removing the limits
    # this is a dirty since it modifies self state
    self.query.clear_limits()
    return self.as_sql(compiler, connection)


def register_expressions():
    Exists.as_teradata = override_exists_as_sql
