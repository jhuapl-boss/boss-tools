#!/usr/local/bin/python3

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

SOCKET_NAME = bossutils.credentials.SOCKET_NAME
SOCKET_BACKLOG = 5

EXIT_SIGNAL = signal.SIGUSR1
PID_FILE = "/var/run/credentials/pid"

# Some of the code for making a daemon taken from
# http://stackoverflow.com/questions/1603109/how-to-make-a-python-script-run-like-a-service-or-daemon-in-linux
# http://code.activestate.com/recipes/278731/
# these both reference
# Stevens' "Advanced Programming in the UNIX Environment"

class Credentials:
    def __init__(self):
        self.log = bossutils.logger.BossLogger()
        self.vault = bossutils.vault.Vault()

        self.aws_path = "aws/creds/" + self.vault.config["system"]["type"]
        self.creds = {}
        self.running = False

        signal.signal(signal.SIGALRM, self.sig_handler)
        signal.signal(EXIT_SIGNAL, self.sig_handler)

        # create socket
        if os.path.exists(SOCKET_NAME):
            os.remove(SOCKET_NAME)

        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.bind(SOCKET_NAME)
        self.sock.listen(SOCKET_BACKLOG)

    def sig_handler(self, signum, frame):
        if signal.SIGALRM == signum:
            self.renew_or_request()
        elif EXIT_SIGNAL == signum:
            raise InterruptedError() # interrupt socket.accept()
        else:
            print("Unhandled signal #" + str(signum))
            self.log.debug("Unhandled signal #" + str(signum))

    def daemonize(self):
        """
        do the UNIX double-fork magic, see Stevens' "Advanced
        Programming in the UNIX Environment" for details (ISBN 0201563177)
        http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
        """
        try:
                pid = os.fork()
                if pid > 0:
                        # exit first parent
                        sys.exit(0)
        except OSError as e:
                sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
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
                sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
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
        self.daemonize()
        self.renew_or_request() # sets alarm timer

        try:
            while True:
                conn, addr = self.sock.accept() # blocks for connections
                print("Connection from {}".format(addr))

                data = json.dumps(self.creds["data"]).encode('utf-8')
                conn.sendall(data)
                conn.close()
        except InterruptedError:
            pass

        # delete the socket
        self.sock.close()
        self.sock = None
        os.remove(SOCKET_NAME)

        # revoke the current credentials
        lease_id = self.creds.get("lease_id")
        if lease_id:
            self.vault.revoke_secret(lease_id)
        self.creds = None

        os.remove(PID_FILE)

    def renew_or_request(self):
        lease_id = self.creds.get("lease_id")
        if lease_id:
            self.creds = self.vault.renew_secret(lease_id)
            # what if the creds are stale and need to be recreated?
        else:
            creds = self.vault.read_dict(self.aws_path, raw=True)
            time.sleep(15) # wait for the credentials to become concurrent with AWS services
            self.creds = creds

        timeout = self.creds.get("lease_timeout")
        timeout = int(int(timeout) * 0.85) # wait for 85% of the lease time
        print("Setting alarm for {} seconds".format(timeout))
        signal.alarm(timeout)

def start():
    Credentials().run()

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