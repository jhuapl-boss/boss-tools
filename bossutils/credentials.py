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