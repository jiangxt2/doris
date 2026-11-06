# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from pathlib import Path

import pyarrow as pa
import pyarrow.orc as orc
import pyarrow.parquet as parquet

root = Path(__file__).parent
tables = {
    "old": pa.table({"id": pa.array([1], pa.int64()), "event": ["click"]}),
    "new": pa.table({"id": pa.array([2], pa.int64()), "event": ["purchase"], "campaign": ["winter"]}),
    "conflict": pa.table({"id": ["not-a-number"], "event": ["click"]}),
    "complex": pa.table({"id": pa.array([3], pa.int64()), "event": ["view"], "tags": [["a"]]}),
    "case_upper": pa.table({"ID": pa.array([3], pa.int64()), "EVENT": ["upper"], "Ä": ["first"]}),
    "case_lower": pa.table({"id": pa.array([4], pa.int64()), "event": ["lower"], "ä": ["second"]}),
    "ascii_upper": pa.table({"ID": pa.array([3], pa.int64()), "EVENT": ["upper"]}),
    "ascii_lower": pa.table({"id": pa.array([4], pa.int64()), "event": ["lower"]}),
}
for name, table in tables.items():
    parquet.write_table(table, root / f"{name}.parquet")
    orc.write_table(table, root / f"{name}.orc")
