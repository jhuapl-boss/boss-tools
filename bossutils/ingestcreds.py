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


from bossutils.vault import Vault
import boto3
import botocore
from . import configuration

# Constants used to generate names of AWS resources.
INGEST_ROLE_NAME = 'aws/roles/ingest{}'     # Uses job id.
INGEST_CREDS_NAME = 'aws/creds/ingest{}'    # Uses job id.
IAM_POLICY_NAME = '{}-ingest_client{}'      # Uses domain and job id.
IAM_PATH = '/{}/ingest/'                    # Uses domain.

class IngestCredentials:
    """Manages temporary AWS credentials used by ingest clients.
    
    Typical usage:
        ingest_creds = IngestCredentials()

        # On POST to endpoint from ingest client.
        arn = ingest_creds.create_policy(policy_doc, job_id)
        ingest_creds.generate_credentials(job_id, arn)

        # On GET to endpoint from ingest client.
        ingest_creds.get_credentials(job_id)

        # When ingest job complete.
        ingest_creds.remove_credentials(job_id)
        ingest_creds.delete_policy(job_id)

    Attributes:
        config (configuration.BossConfig): Boss configuration.
        domain (string): Domain this class is running in.  Used for naming.
        iam (IAM.ServiceResource): AWS interface to IAM.
        vault (bossutils.Vault): Wrapper to access Vault secret store.
    """

    def __init__(self, config=None, region_name='us-east-1'):
        """
        Args:
            config (optional[configuration.BossConfig]): Boss configuration.  Defaults to loading from /etc/boss/boss.config.
            region_name (optional[string]): AWS region to use.  Defaults to us-east-1.
        """
        if config is None:
            self.config = configuration.BossConfig()
        else:
            self.config = config
        self.vault = Vault(config)
        # Get the domain the endpoint lives in.
        self.domain = self.config['system']['fqdn'].split('.', 1)[1]
        self.iam = boto3.Resource('iam', region_name=region_name,
            aws_access_key_id='AKIAJFH5RRFJKEOJIGXA', 
            aws_secret_access_key='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')

    def create_policy(self, policy_document, job_id, description=''):
        """Create a new IAM policy for the given ingest job.

        Args:
            policy_document (dict):
            job_id (int): Id of ingest job used for name of Vault role.
            description (optional[string]): Policy description, defaults to empty string.

        Returns:
            (string): New policy's ARN.
        """
        sanitized_domain = self.domain.replace('.', '-')
        path=IAM_PATH.format(sanitized_domain)

        resp = self.iam.create_policy(
            PolicyName=IAM_POLICY_NAME.format(job_id),
            PolicyDocument=json.dumps(policy),
            Path=path,
            Description=description)
        return resp['Policy']['Arn']

    def delete_policy(self, job_id):
        """Delete the IAM policy associated with an ingest job.
        
        Args:
            job_id (int): Id of ingest job used for name of Vault role.

        Returns:
            (bool): False if policy not found.
        """
        sanitized_domain = self.domain.replace('.', '-')
        path=IAM_PATH.format(sanitized_domain)
        name = IAM_POLICY_NAME.format(job_id)
        for policy in iam.policies.filter(Scope='Local', PathPrefix=path):
            if policy.attributes['policy_name'] == name:
                self.iam.delete_policy(PolicyArn=policy.arn)
                return True

        return False

    def generate_credentials(self, job_id, iam_policy_arn):
        """Generate temporary credentials for an ingest job.

        A temporary Vault role mapped to an IAM policy is created.

        Args:
            job_id (int): Id of ingest job used for name of Vault role.
            iam_policy_arn (string): Policy associated with credentials.
        Returns:
            (dict): Contains AWS key and secret key.
        """

        # Create Vault role and associate with an IAM policy.
        sanitized_domain = self.domain.replace('.', '-')
        role_path = INGEST_ROLE_NAME.format(sanitized_domain, job_id)
        self.vault.write(role_path, arn=iam_policy_arn) 

        # Generate temporary credentials for that role.
        creds_path = INGEST_CREDS_NAME.format(sanitized_domain, job_id)
        return self.vault.read_dict(creds_path, raw=True)

    def get_credentials(self, job_id):
        """Get new temporary credentials for the given ingest job.

        Before calling get_credentials(), the Vault role must be created by 
        generate_credentials() for the given job_id.

        Args:
            job_id (int): Get new credentials for this ingest job.

        Returns:
            (dict): Contains AWS key and secret key.
        """
        sanitized_domain = self.domain.replace('.', '-')
        path = INGEST_CREDS_NAME.format(sanitized_domain, job_id)
        return self.vault.read_dict(path, raw=True)

    def remove_credentials(self, job_id):
        """Revoke credentials and delete the Vault role associated with an ingest job.

        This call cleans up for get_credentials() and generate_credentials().

        Args:
            job_id (int): Get new credentials for this ingest job.
        """
        sanitized_domain = self.domain.replace('.', '-')
        creds_path = INGEST_CREDS_NAME.format(sanitized_domain, job_id)
        self.vault.revoke_secret_prefix(creds_path)

        role_path = INGEST_ROLE_NAME.format(sanitized_domain, job_id)
        self.vault.delete(role_path)

