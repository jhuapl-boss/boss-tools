import json
import socket

SOCKET_NAME = "/var/run/credentials/sock"


def get_credentials():
    """
    Method to query the credential service for AWS credentials in JSON format
    :return: Secret and access tokens
    :rtype: dict
    """
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(SOCKET_NAME)
    data = b""
    while True: # just to be safe
        tmp = sock.recv(1024)
        if tmp:
            data += tmp
        else:
            break

    sock.close()
    return json.loads(data.decode('utf-8'))