#!/bin/bash

# Set this environment variable so non-unit tests can be skipped.
export NOSE_UNIT_TESTS_RUNNING=1
nose2
