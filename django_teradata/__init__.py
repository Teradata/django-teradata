__version__ = '4.2a1'

# Check Django compatibility before other imports which may fail if the
# wrong version of Django is installed.
from .utils import check_django_compatibility

check_django_compatibility()

