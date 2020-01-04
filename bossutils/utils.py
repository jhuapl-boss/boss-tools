# Copyright 2016 The Johns Hopkins University Applied Physics Laboratory
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
DYNAMIC_URL = "http://169.254.169.254/latest/dynamic/"

def read_url(url):
    """Read the data from the given url and return it as a utf-8 string."""
    return urllib.request.urlopen(url).read().decode("utf-8")

def execute(cmd, whole=False, shell=False):
    """
    Execute the given command on the system.
    Args:
        cmd: command to execute
        whole: if true don't split the command into parts, execute as a whole command.  Defaults to False to be compatible with previous uses of the function.
        shell: run the command through a shell which allows Pipes to work.  Defaults to False to be compatible with previous uses of the function.

    Returns:
        (int) Return code from command executed.
    """
    log = logger.bossLogger()
    log.info("Executing command: {}".format(cmd))
    if whole:
        proc = subprocess.Popen(cmd,
                                shell=shell,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
    else:
        proc = subprocess.Popen(shlex.split(cmd),
                                shell=shell,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)

    # Right now use communicate, no big out process should
    # be executed by this.
    stdout, stderr = proc.communicate()
    if stdout:
        for line in stdout.split(b"\n"):
            log.debug("STDOUT: {}".format(line))
    if stderr:
        for line in stderr.split(b"\n"):
            log.debug("STDERR: {}".format(line))

    return proc.returncode

def proc_name():
    argv = sys.argv[0]
    real = os.path.realpath(argv)
    name = os.path.basename(real)
    return name

def stop_firstboot():
    execute("/usr/sbin/update-rc.d -f {} remove".format(proc_name()))

def set_excepthook():
    log = logger.bossLogger()
    name = proc_name()

    def ex_handler(ex_cls, ex, tb):
        log.critical(name + ": " + ''.join(traceback.format_tb(tb)))
        log.critical('{}: {}: {}'.format(name, ex_cls, ex))

    sys.excepthook = ex_handler
    log.debug("{}: Configured sys.excepthook".format(name))
