"""Helper functions for communicating with AWS.
"""

import time
import boto3
from . import vault
from . import utils

def create_session():
    """Create a boto3 session. Dynamically created AWS credentials from
    Vault and sets the region name to that of the running machine.
    
    NOTE: the current VM must have read permissions on 'aws/creds/<machine_type'
    NOTE: This command will take several seconds to return to make sure that
          the dynamically created AWS credentials are valid.
    """
    v = vault.Vault()
    path = "aws/creds/" + v.config["system"]["type"]
    creds = v.read_dict(path)
    
    # Unfortunately, IAM credentials are eventually
    # consistent with respect to other Amazon services
    # Need a sleep to make sure the session is usable
    # after being returned
    time.sleep(15)
    
    region = utils.read_url(utils.METADATA_URL + "placement/availability-zone")
    
    session = boto3.session.Session(aws_access_key_id = creds["access_key"],
                                    aws_secret_access_key = creds["secret_key"],
                                    region_name = region)
    return session