import urllib.request
import subprocess
import shlex

USERDATA_URL = "http://169.254.169.254/latest/user-data"
METADATA_URL = "http://169.254.169.254/latest/meta-data/"

def read_url(url):
    return urllib.request.urlopen(url).read().decode("utf-8")
    
def execute(cmd):
    subprocess.call(shlex.split(cmd))