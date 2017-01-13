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

from heaviside.activities import ActivityManager, ActivityProcess, TaskProcess

from bossutils.delete import *

bossutils.utils.set_excepthook()

def Pass(input_):
    return input_

class BossActivityManager(ActivityManager):
    def __init__(self):
        super().__init__()
        config = bossutils.configuration.BossConfig()

        self.domain = config['system']['fqdn'].split('.', 1)[1]

    def build(self):
        def dispatch(target):
            def wrapped(*args, **kwargs):
                return TaskProcess(*args, target=target, **kwargs)
            return wrapped

        return [
            #lambda: ActivityProcess('Name.'+self.domain, dispatch(function))
            lambda: ActivityProcess('Pass.'+self.domain, dispatch(Pass)),
            lambda: ActivityProcess('delete_test_1.' + self.domain, dispatch(delete_test_1)),
            lambda: ActivityProcess('delete_test_2.' + self.domain, dispatch(delete_test_2)),
            lambda: ActivityProcess('delete_test_3.' + self.domain, dispatch(delete_test_3)),
        ]

if __name__ == '__main__':
    mgr = BossActivityManager()
    mgr.run()

