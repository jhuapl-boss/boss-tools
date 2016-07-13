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
# Short-Description: Base Daemon use by overriding run() method and passing a unique pid file.
# Description:
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

# Some of the code for making a daemon is based on
# http://stackoverflow.com/questions/1603109/how-to-make-a-python-script-run-like-a-service-or-daemon-in-linux
# http://code.activestate.com/recipes/278731/
# these both reference
# Stevens' "Advanced Programming in the UNIX Environment"

class DaemonBase:
    """
    Base Daemon used to create new daemons by subclassing this class.
    override run() method to perform daemon work
    subclass  should also contain
        if __name__ == '__main__':
            SubClassName("subclass-daemon.pid").main()

    """
    def __init__(self, pid_file_name, pid_dir="/var/run"):
        self.pid_file = os.path.join(pid_dir, pid_file_name)
        self.log = bossutils.logger.BossLogger().logger
        self.running = False
        self.pid = None

    def convert_to_demon(self):
        """
        do the UNIX double-fork magic, see Stevens' "Advanced
        Programming in the UNIX Environment" for details (ISBN 0201563177)
        http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
        """

        self.log.debug("Daemonizing")

        try:
                self.pid = os.fork()
                if self.pid > 0:
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
                self.pid = os.fork()
                if self.pid > 0:
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
        with open(self.pid_file, "w+") as fh:
            fh.write("{}\n".format(os.getpid()))

    def run(self):
        """
        This method should be overridden by the child class daemon to perform some actions.
        Returns:

        """
        while True:
            time.sleep(30)
            self.log.info("action occured in DaemonBase - run() method should be overridden.")

    def start(self):
        """
        method called when daemon is started up
        Returns:

        """
        self.convert_to_demon()

        # Place hook after daemonizing, as I am unsure
        # if it will transfer after the two forks
        bossutils.utils.set_excepthook() # help with catching errors

        self.run()
        os.remove(self.pid_file)

    def stop(self):
        """
        method called  when the daemon is stopped
        Returns:

        """
        os.kill(self.pid, EXIT_SIGNAL)

    # from http://stackoverflow.com/questions/568271/how-to-check-if-there-exists-a-process-with-a-given-pid
    def pid_exists(self, pid):
        """
        function to test if a process exists
        Args:
            pid:

        Returns:

        """
        if pid < 0: return False #NOTE: pid == 0 returns True
        try:
            os.kill(pid, 0)
        except ProcessLookupError: # errno.ESRCH
            return False # No such process
        except PermissionError: # errno.EPERM
            return True # Operation not permitted (i.e., process exists)
        else:
            return True # no error, we can send a signal to the process


    def main(self):
        """ basic main fucntion for a daemon.
            sys.argv[1] should be start, stop, restart or status.

        Returns:

        """
        usage = "Usage: {} (start|stop|restart|status)".format(sys.argv[0])
        if len(sys.argv) != 2:
            print(usage)
            sys.exit(1)

        if os.path.exists(self.pid_file):
            with open(self.pid_file, "r") as fh:
                self.pid = int(fh.read().strip())

            if not self.pid_exists(self.pid):
                os.remove(self.pid_file)
                self.pid = None
        else:
            self.pid = None

        action = sys.argv[1]
        if action in ("start",):
            if self.pid is None:
                self.start()
        elif action in ("stop",):
            if self.pid is not None:
                self.stop()
        elif action in ("restart",):
            if self.pid is not None:
                self.stop()
            self.start()
        elif action in ("status",):
            if self.pid is None:
                print("Daemon is not running")
            else:
                print("Daemon is running (pid {})".format(self.pid))
        else:
            print(usage)
            sys.exit(1)


if __name__ == '__main__':
    DaemonBase("daemon-base.pid").main()
