import math

from django.db.models.functions import Pi


def pi(self, compiler, connection, **extra_context):
    return self.as_sql(
        compiler, connection, template=str(math.pi), **extra_context
    )


def register_functions():
    Pi.as_teradata = pi
