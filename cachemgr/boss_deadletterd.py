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
# Short-Description: Service for the Dead Letter Daemon on CacheManager
# Description: Service for the Dead Letter Daemon on CacheManager
#
### END INIT INFO

import time

from bossutils import daemon_base


class DeadLetterDaemon(daemon_base.DaemonBase):

    def run(self):
        while True:
            time.sleep(30)
            self.log.info("action occured in boss-deadletterd.")

if __name__ == '__main__':
    DeadLetterDaemon("boss-deadletterd.pid").main()