import os
import json
import socket

SOCKET_NAME = "/var/run/credentials/sock"

def get_credentials():
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(SOCKET_NAME)
    data = b""
    while True: # just to be safe
        tmp = sock.recv(1024)
        if tmp:
            data += tmp
        else:
            break
    return json.loads(data.decode('utf-8'))