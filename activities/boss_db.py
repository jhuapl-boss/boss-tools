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
                           # Don't turn off autocommit w/o visiting every user
                           # of this connection and ensuring that they use a
                           # transaction!
                           autocommit=True,
                           charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)

def update_downsample_status_in_db(args):
    """
    Update the downsample status in the MySQL database.

    This supports a state of the resolution hierarchy step function.

    This function is tested in
    boss.git/django/bossspatialdb/test/test_update_downsample_status.py.
    Tests live there because Django owns the DB.

    Args:
        args (dict):
            db_host (str): MySQL host name.
            channel_id (int): ID of channel for downsample.
            status (str): String from DownsampleStatus class.

    Returns:
        (dict): Returns input args for passing to next SFN state.
    """
    sql = """
        UPDATE channel
        SET downsample_status = %(status)s
        WHERE id = %(chan_id)s
        """

    db_host = args['db_host']
    chan_id = args['channel_id']
    status = args['status']

    sql_args = dict(status=status, chan_id=str(chan_id))

    try:
        db_connection = get_db_connection(db_host)
        with db_connection.cursor(pymysql.cursors.SSCursor) as cursor:
            rows = cursor.execute(sql, sql_args)
            if rows < 1:
                LOG.error(
                    f'DB said no rows updated when trying to set downsample status to {status} for channel {chan_id}'
                )
    except Exception as ex:
        LOG.exception(f'Failed to set downsample status to {status} for channel {chan_id}: {ex}')

    return args

def set_downsample_arn_in_db(args):
    """
    Set the arn of the running downsample step function in the MySQL database.

    This supports a state of the resolution hierarchy step function.

    This function is tested in
    boss.git/django/bossspatialdb/test/test_set_downsample_arn.py.
    Tests live there because Django owns the DB.

    Args:
        args (dict):
            db_host (str): MySQL host name.
            channel_id (int): ID of channel for downsample.
            exe_sfn_arn (str): ARN of running downsample step function.

    Returns:
        (dict): Returns input args for passing to next SFN state.
    """
    sql = """
        UPDATE channel
        SET downsample_arn = %(arn)s
        WHERE id = %(chan_id)s
        """

    db_host = args['db_host']
    chan_id = args['channel_id']
    arn = args['exe_sfn_arn']

    sql_args = dict(arn=arn, chan_id=str(chan_id))

    try:
        db_connection = get_db_connection(db_host)
        with db_connection.cursor(pymysql.cursors.SSCursor) as cursor:
            rows = cursor.execute(sql, sql_args)
            if rows < 1:
                LOG.error(
                    f'DB said no rows updated when trying to set downsample arn for channel {chan_id}'
                )
    except Exception as ex:
        LOG.exception(f'Failed to set downsample arn for channel {chan_id}: {ex}')

    return args
