# -*- coding: utf-8 -*-
# Copyright 2024 Michael Bungenstock
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
#
import unittest
from mib import bq
import datetime as dt
from decimal import Decimal


class TestBasic(unittest.TestCase):
    def setUp(self):
        return super().setUp()

    def test_simple(self):
        data = {
            "first": "Michael",
            "last": "Jackson",
            "sub": {"street": "Friedrich-Ebert-Damm"},
            "awards": ["winner", "gold"],
            "complex": [{"description": "good"}, {"description": "world"}],
            "start": dt.datetime.now(),
            "day": dt.datetime.today(),
            "flag": True,
            "value": Decimal("-9.876e-3"),
            "nest": {"deep": {"very": {"ende": "nice"}}},
        }
        nor = 10
        rows = [data for i in range(int(nor))]
        bq.write_single_batch("vertexit.demo_ds.simple", rows)


if __name__ == "__main__":
    unittest.main()
