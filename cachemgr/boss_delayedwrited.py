#!/usr/local/bin/python3

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

### BEGIN INIT INFO
# Provides: credentials
# Required-Start:
# Required-Stop:
# Default-Start: 2 3 4 5
# Default-Stop:
# Short-Description: Service to maintain the delayed write daemon on CacheManager
# Description: Service to maintaining Delayed Write Daemon on CacheManager
#
### END INIT INFO

import time

from bossutils import daemon_base
from bossutils import configuration

#from spatialdb import SpatialDB


class DelayedWriteDaemon(daemon_base.DaemonBase):

    def run(self):
        # Setup SPDB instance
        #config = configuration.BossConfig()
        #kvio_config = {"cache_host": config['aws']['cache'],
        #               "cache_db": 0,
        #               "read_timeout": 86400}
#
        #sp = SpatialDB(event["config"]["kv_config"],
        #               event["config"]["state_config"],
        #               event["config"]["object_store_config"])

        while True:
            # Get All delayed writes
            #delay_write_keys




            time.sleep(10)
            self.log.info("action occured in boss-delayedwrite.")

if __name__ == '__main__':
    DelayedWriteDaemon("boss-delayedwrited.pid").main()
