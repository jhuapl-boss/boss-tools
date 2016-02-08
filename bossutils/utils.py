"""Common utilities used by the different parts of the library.

USERDATA_URL is the url to AWS user-data
METADATA_URL is the url prefix for AWS meta-data
"""

import urllib.request
import subprocess
import shlex

USERDATA_URL = "http://169.254.169.254/latest/user-data"
METADATA_URL = "http://169.254.169.254/latest/meta-data/"

def read_url(url):
    """Read the data from the given url and return it as a utf-8 string."""
    return urllib.request.urlopen(url).read().decode("utf-8")
    
def execute(cmd):
    """Execute the given command on the system."""
    subprocess.call(shlex.split(cmd))