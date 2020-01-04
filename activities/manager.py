#!/usr/bin/env python3
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

import bossutils
bossutils.utils.set_excepthook()
bossutils.logger.configure()
log = bossutils.logger.bossLogger()

from heaviside.activities import ActivityManager

import delete_cuboid as dc

#import populate_upload_queue as puq
import ingest_queue_populate as iqp
import resolution_hierarchy as rh

class BossActivityManager(ActivityManager):
    def __init__(self):
        super().__init__()
        config = bossutils.configuration.BossConfig()

        # DP NOTE: make activity names Lambda compatible
        self.domain = '-' + config['system']['fqdn'].split('.', 1)[1].replace('.', '-')
        key = lambda x: x + self.domain

        self.activities = {
            # Query for Deletes StepFunction
            key('query_for_deletes') : dc.query_for_deletes,

            # Delete Cuboid StepFunction
            key('delete_metadata') : dc.delete_metadata,
            key('delete_id_count') : dc.delete_id_count,
            key('delete_id_index') : dc.delete_id_index,
            key('merge_parallel_outputs') : dc.merge_parallel_outputs,
            key('find_s3_index') : dc.find_s3_index,
            key('delete_s3_index') : dc.delete_s3_index,
            key('notify_admins') : dc.notify_admins,
            key('delete_clean_up') : dc.delete_clean_up,

            # # Delete Collection
            # key('delete_metadata') : dc.delete_metadata,
            # key('delete_collection') : dc.delete_collection,
            #
            # # Delete Coordinate Frame
            # key('delete_metadata') : dc.delete_metadata,
            # key('delete_coordinate_frame') : dc.delete_coordinate_frame,
            #
            # # Delete Experiment
            # key('delete_metadata') : dc.delete_metadata,
            key('delete_experiment') : dc.delete_experiment,
            key('delete_collection') : dc.delete_collection,
            key('delete_coordinate_frame') : dc.delete_coordinate_frame,

            # Populate Upload Queue StepFunction
            key('IngestPopulate') : iqp.ingest_populate,
            key('VerifyCount') : iqp.verify_count,

            # Resolution Hierarchy StepFunction
            key('DownsampleChannel') : rh.downsample_channel,
        }

if __name__ == '__main__':
    mgr = BossActivityManager()
    log.info("Starting activity manager server")
    mgr.run()

