# Copyright 2013-2016 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
This module contains utilities for generating timestamps for client-side
timestamp specification.
"""

import logging
import time
from threading import Lock

log = logging.getLogger(__name__)

class MonotonicTimestampGenerator(object):
    """
    An object that, when called, returns `time.time() * 1e6` when possible,
    but, if the value returned by `time.time` doesn't increase, drifts into the
    future and logs warnings.
    """
    def __init__(self):
        self.lock = Lock()
        with self.lock:
            self.last = 0

    def next_timestamp(self):
        with self.lock:
            now = time.time() * 1e6
            if now > self.last:
                self.last = now
                return now
            else:
                log.warn(
                    "Clock skew detected: current tick ({now}) was {diff} "
                    "microseconds behind the last generated timestamp "
                    "({last}), returned timestamps will be artificially "
                    "incremented to guarantee monotonicity.".format(
                        now=now, diff=(self.last - now), last=self.last))
                self.last += 1
                return self.last

    def __call__(self):
        return self.next_timestamp()
