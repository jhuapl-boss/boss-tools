#!/usr/local/bin/python3

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

### BEGIN INIT INFO
# Provides: credentials
# Required-Start:
# Required-Stop:
# Default-Start: 2 3 4 5
# Default-Stop:
# Short-Description: Service to maintaining AWS credentials from Vault
# Description: Service to maintaining AWS credentials from Vault by
#              automatically renewing the credentials before they expire.
#
### END INIT INFO

import sys
import os
import json
import socket
import signal
import time

import bossutils

EXIT_SIGNAL = signal.SIGUSR1
PID_FILE = "/var/run/prefetchd/pid"

# Some of the code for making a daemon taken from
# http://stackoverflow.com/questions/1603109/how-to-make-a-python-script-run-like-a-service-or-daemon-in-linux
# http://code.activestate.com/recipes/278731/
# these both reference
# Stevens' "Advanced Programming in the UNIX Environment"

class CacheManager:
    def __init__(self):
        self.log = bossutils.logger.BossLogger().logger
        #self.vault = bossutils.vault.Vault()

        # self.aws_path = "aws/creds/" + self.vault.config["system"]["type"]
        # self.creds = {}
        self.running = False


    def convert_to_demon(self):
        """
        do the UNIX double-fork magic, see Stevens' "Advanced
        Programming in the UNIX Environment" for details (ISBN 0201563177)
        http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
        """

        self.log.debug("Daemonizing")

        try:
                pid = os.fork()
                if pid > 0:
                        # exit first parent
                        sys.exit(0)
        except OSError as e:
                self.log.error("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
                sys.exit(1)

        # decouple from parent environment
        os.chdir("/")
        os.setsid()
        os.umask(0)

        # do second fork
        try:
                pid = os.fork()
                if pid > 0:
                        # exit from second parent
                        sys.exit(0)
        except OSError as e:
                self.log.error("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
                sys.exit(1)

        # redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        for fileno in [0,1,2]:
            os.close(fileno)
        os.open("/dev/null", os.O_RDWR)
        os.dup2(0, 1)
        os.dup2(0, 2)


        # write pidfile
        with open(PID_FILE, "w+") as fh:
            fh.write("{}\n".format(os.getpid()))

    def run(self):
        self.convert_to_demon()

        # Place hook after daemonizing, as I am unsure
        # if it will transfer after the two forks
        bossutils.utils.set_excepthook() # help with catching errors

        while True:
            time.sleep(30)
            self.log.error("action occured.")

        os.remove(PID_FILE)


def start():
    CacheManager().run()

def stop():
    os.kill(pid, EXIT_SIGNAL)

# from http://stackoverflow.com/questions/568271/how-to-check-if-there-exists-a-process-with-a-given-pid
def pid_exists(pid):
    if pid < 0: return False #NOTE: pid == 0 returns True
    try:
        os.kill(pid, 0)
    except ProcessLookupError: # errno.ESRCH
        return False # No such process
    except PermissionError: # errno.EPERM
        return True # Operation not permitted (i.e., process exists)
    else:
        return True # no error, we can send a signal to the process

if __name__ == '__main__':
    usage = "Usage: {} (start|stop|restart|status)".format(sys.argv[0])
    if len(sys.argv) != 2:
        print(usage)
        sys.exit(1)

    if os.path.exists(PID_FILE):
        with open(PID_FILE, "r") as fh:
            pid = int(fh.read().strip())

        if not pid_exists(pid):
            os.remove(PID_FILE)
            pid = None
    else:
        pid = None

    action = sys.argv[1]
    if action in ("start",):
        if pid is None:
            start()
    elif action in ("stop",):
        if pid is not None:
            stop()
    elif action in ("restart",):
        if pid is not None:
            stop()
        start()
    elif action in ("status",):
        if pid is None:
            print("Daemon is not running")
        else:
            print("Daemon is running (pid {})".format(pid))
    else:
        print(usage)
        sys.exit(1)
