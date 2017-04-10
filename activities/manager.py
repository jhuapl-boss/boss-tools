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
log = bossutils.logger.BossLogger().logger

from heaviside.activities import ActivityManager, ActivityProcess, TaskProcess

from delete_cuboid import *

#import populate_upload_queue as puq
import ingest_queue_populate as iqp
import resolution_hierarchy as rh

class BossActivityManager(ActivityManager):
    def __init__(self):
        super().__init__()
        config = bossutils.configuration.BossConfig()

        # DP NOTE: make activity names Lambda compatible
        self.domain = '-' + config['system']['fqdn'].split('.', 1)[1].replace('.', '-')

    def build(self):
        def dispatch(target):
            def wrapped(*args, **kwargs):
                return TaskProcess(*args, target=target, **kwargs)
            return wrapped

        return [
            #lambda: ActivityProcess('Name'+self.domain, dispatch(function))
            # lambda: ActivityProcess('delete_test_1' + self.domain, dispatch(delete_test_1)),
            # lambda: ActivityProcess('delete_test_2' + self.domain, dispatch(delete_test_2)),
            # lambda: ActivityProcess('delete_test_3' + self.domain, dispatch(delete_test_3)),
            # lambda: ActivityProcess('delete_test_4' + self.domain, dispatch(delete_test_4)),

            # Query for Deletes StepFunction
            lambda: ActivityProcess('query_for_deletes' + self.domain, dispatch(query_for_deletes)),

            # Delete Cuboid StepFunction
            lambda: ActivityProcess('delete_metadata' + self.domain, dispatch(delete_metadata)),
            lambda: ActivityProcess('delete_id_count' + self.domain, dispatch(delete_id_count)),
            lambda: ActivityProcess('delete_id_index' + self.domain, dispatch(delete_id_index)),
            lambda: ActivityProcess('merge_parallel_outputs' + self.domain, dispatch(merge_parallel_outputs)),
            lambda: ActivityProcess('find_s3_index' + self.domain, dispatch(find_s3_index)),
            lambda: ActivityProcess('delete_s3_index' + self.domain, dispatch(delete_s3_index)),
            lambda: ActivityProcess('notify_admins' + self.domain, dispatch(notify_admins)),
            lambda: ActivityProcess('delete_clean_up' + self.domain, dispatch(delete_clean_up)),

            # # Delete Collection
            # lambda: ActivityProcess('delete_metadata' + self.domain, dispatch(delete_metadata)),
            # lambda: ActivityProcess('delete_collection' + self.domain, dispatch(delete_collection)),
            #
            # # Delete Coordinate Frame
            # lambda: ActivityProcess('delete_metadata' + self.domain, dispatch(delete_metadata)),
            # lambda: ActivityProcess('delete_coordinate_frame' + self.domain, dispatch(delete_coordinate_frame)),
            #
            # # Delete Experiment
            #lambda: ActivityProcess('delete_metadata' + self.domain, dispatch(delete_metadata)),
            lambda: ActivityProcess('delete_experiment' + self.domain, dispatch(delete_experiment)),
            lambda: ActivityProcess('delete_collection' + self.domain, dispatch(delete_collection)),
            lambda: ActivityProcess('delete_coordinate_frame' + self.domain, dispatch(delete_coordinate_frame)),

            # Populate Upload Queue StepFunction
            #lambda: ActivityProcess('PopulateQueue' + self.domain, dispatch(puq.populate_upload_queue)),
            lambda: ActivityProcess('IngestPopulate' + self.domain, dispatch(iqp.ingest_populate)),
            lambda: ActivityProcess('VerifyCount' + self.domain, dispatch(iqp.verify_count)),

            # Resolution Hierarchy StepFunction
            lambda: ActivityProcess('DownsampleChannel' + self.domain, dispatch(rh.downsample_channel))
        ]

if __name__ == '__main__':
    mgr = BossActivityManager()
    log.info("Starting activity manager server")
    mgr.run()

