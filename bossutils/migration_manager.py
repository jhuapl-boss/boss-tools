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

"""
migration_manager.py is meant to be run on the endpoint to pull down the latest migration changes from s3
"""

import os
import sys
import bossutils
import boto3
import argparse
import json


MIGRATION_BUCKET_DEV = "migrations-dev-boss"
MIGRATION_BUCKET_PROD = "migrations-prod-boss"

bucketLookup = {"endpoint.integration.boss": MIGRATION_BUCKET_PROD,
                "endpoint.production.boss": MIGRATION_BUCKET_PROD,
                "default": MIGRATION_BUCKET_DEV}

COMMANDS=["get", "put"]

MIGRATIONS = ["bossobject", "bossingest", "bosstiles", "bosscore", "bossmeta", "sso", "mgmt"]
DJANGO_ROOT = "/srv/www/django";

class MigrationManager:
    """
    gets django migrations from s3 and populates the migration directory in the local django_root
    puts local django migrations files in s3.
    Will not delete files but will update files with new versions.
    When deleting a cloudformation stack the django migration files should be deleted from s3 for the same stack.
    """

    def __init__(self):
        config = bossutils.configuration.BossConfig()
        self.key = config["system"]["fqdn"]

        if self.key in bucketLookup.keys():
            self.bucket_name = bucketLookup[self.key]
        else:
            self.bucket_name = bucketLookup["default"]
        self.django_root_dir = DJANGO_ROOT

    def get_migrations(self):
        cmd = "aws --region {region} s3 sync s3://{bucket}/{key} {local_django}".format(
            region=bossutils.aws.get_region(),
            bucket=self.bucket_name,
            key=self.key,
            local_django=self.django_root_dir)
        resp = bossutils.utils.execute(cmd)
        return int(resp) == 0

    def put_migrations(self):
        all_worked = True
        for migration_dir in MIGRATIONS:
            cmd = "aws --region {region} s3 sync {local_django}/{migrations}/migrations s3://{bucket}/{key}/{migrations}/migrations".format(
                region=bossutils.aws.get_region(),
                bucket=self.bucket_name,
                key=self.key,
                local_django=self.django_root_dir,
                migrations=migration_dir)
            resp = bossutils.utils.execute(cmd)
            if int(resp) != 0:
                all_worked = False
        return all_worked

    def compare_migrations(self):
        pass



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Test of migration_manager",
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog='Test out migration manager')
    parser.add_argument("command",
                        choices=COMMANDS,
                        help="")
    args = parser.parse_args()

    mm = MigrationManager()

    if args.command == "get":
        print(mm.get_migrations())
    elif args.command == "put":
        print(mm.put_migrations())
    else:
        print("Unknown Command: " + args.command)




