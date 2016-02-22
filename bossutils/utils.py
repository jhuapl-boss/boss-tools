"""Common utilities used by the different parts of the library.

USERDATA_URL is the url to AWS user-data
METADATA_URL is the url prefix for AWS meta-data
"""

import urllib.request
import subprocess
import shlex
import os
import sys
import traceback
from . import logger

USERDATA_URL = "http://169.254.169.254/latest/user-data"
METADATA_URL = "http://169.254.169.254/latest/meta-data/"

def read_url(url):
    """Read the data from the given url and return it as a utf-8 string."""
    return urllib.request.urlopen(url).read().decode("utf-8")

def execute(cmd):
    """Execute the given command on the system."""
    log = logger.BossLogger()
    log.info("Executing command: {}".format(cmd))
    proc = subprocess.Popen(shlex.split(cmd),
                            stdout = subprocess.PIPE,
                            stderr = subprocess.PIPE)

    # Right now use communicate, no big out process should
    # be executed by this.
    stdout, stderr = proc.communicate()
    if stdout:
        for line in stdout.split(b"\n"):
            log.debug("STDOUT: {}".format(line))
    if stderr:
        for line in stderr.split(b"\n"):
            log.debug("STDERR: {}".format(line))

def proc_name():
    argv = sys.argv[0]
    real = os.path.realpath(argv)
    name = os.path.basename(real)
    return name

def stop_firstboot():
    execute("/usr/sbin/update-rc.d -f {} remove".format(proc_name()))

def set_excepthook():
    log = logger.BossLogger()
    name = proc_name()

    def ex_handler(ex_cls, ex, tb):
        log.critical(name + ": " + ''.join(traceback.format_tb(tb)))
        log.critical('{}: {}: {}'.format(name, ex_cls, ex))

    sys.excepthook = ex_handler
    log.debug("{}: Configured sys.excepthook".format(name))