#!/bin/bash

# Set this environment variable so non-unit tests can be skipped.
export NOSE_UNIT_TESTS_RUNNING=1

# Set path to dependencies.
set PYTHONPATH=$WORKSPACE/../../spdb/workspace:$WORKSPACE/../../ndingest/workspace
nose2
