# Copyright 2020 The Johns Hopkins University Applied Physics Laboratory
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pymysql.cursors
import bossutils

LOG = bossutils.logger.bossLogger()

def get_db_connection(host):
    """
    Connects to vault to get database information and then makes a DB connection.

    Note that the connection is opened with auto-commit turned ON.

    Args:
        host (str): Host name of database.

    Returns:
        (pymysql.Connection) connection to DB
    """
    vault = bossutils.vault.Vault()
    LOG.debug("get_db_connection(): made connection to Vault")

    # ------ get values from Vault -----------
    user = vault.read('secret/endpoint/django/db', 'user')
    password = vault.read('secret/endpoint/django/db', 'password')
    db_name = vault.read('secret/endpoint/django/db', 'name')
    port = int(vault.read('secret/endpoint/django/db', 'port'))

    # ---- debug locally -------
    # host = "localhost"
    # user = "testuser"
    # password = ""
    # db_name = "boss"
    # port = 3306

    return pymysql.connect(host=host,
                           user=user,
                           password=password,
                           db=db_name,
                           port=port,
                           autocommit=True,
                           charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)
