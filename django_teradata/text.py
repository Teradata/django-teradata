from django.db.models.functions.text import Left


def left_as_teradata(self, compiler, connection, **extra_context):
    return self.get_substr().as_sql(compiler, connection, **extra_context)


def register_text():
    Left.as_teradata = left_as_teradata
