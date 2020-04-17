#import explicitly, as entrypoint not present during test  (only after setup.py and dist file created)
from pytest_mproc.plugin import *  # noqa