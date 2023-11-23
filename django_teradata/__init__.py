__version__ = '4.2a1'

# Check Django compatibility before other imports which may fail if the
# wrong version of Django is installed.
from .utils import check_django_compatibility

check_django_compatibility()

from .expressions import register_expressions  # noqa
from .functions import register_functions  # noqa
from .text import register_text  # noqa

register_expressions()
register_functions()
register_text()
