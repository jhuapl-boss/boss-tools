# Copyright 2016 The Johns Hopkins University Applied Physics Laboratory
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# lambdafcns contains symbolic links to lambda functions in boss-tools/lambda.
# Since lambda is a reserved word, this allows importing from that folder 
# without updating scripts responsible for deploying the lambda code.
from lambdafcns.downsample_volume import (
    S3DynamoDBTable, downsample_volume, S3Bucket
)

from bossutils.multidimensional import XYZ
import numpy as np
import unittest
from unittest.mock import patch

class TestDownsampleVolumeLambda(unittest.TestCase):

    @patch('lambdafcns.downsample_volume.S3DynamoDBTable', autospec=True)
    @patch('lambdafcns.downsample_volume.S3Bucket', autospec=True)
    @patch('blosc.decompress', autospec=True)
    def test_downsample_volume(self, fake_decompress, fake_s3, fake_s3_ind):
        """
        Just execute the majority of the code in downsample_volume() to catch
        typos and other errors that might show up at runtime.
        """
        fake_s3.get.return_value = None
        fake_decompress.return_value = np.random.randint(
            0, 256, (16, 512, 512), dtype='uint64')

        args = dict(
            collection_id=1,
            experiment_id=2,
            channel_id=3,
            annotation_channel=True,
            data_type='uint64',
            s3_bucket='testBucket.example.com',
            s3_index='s3index.example.com',
            resolution=0,
            type='isotropic',
            iso_resolution=4,
            aws_region='us-east-1'
        )
        target = XYZ(0, 0, 0)
        step = XYZ(2, 2, 2)
        dim = XYZ(512, 512, 16)
        use_iso_key = True

        downsample_volume(args, target, step, dim, use_iso_key)



