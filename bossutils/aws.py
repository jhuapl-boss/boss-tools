import time
import boto3
from . import vault
from . import utils

def create_session():
    v = vault.Vault()
    path = "/aws/creds/" + v.config["system"]["type"]
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