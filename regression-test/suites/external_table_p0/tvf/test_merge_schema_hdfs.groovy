// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

suite("test_merge_schema_hdfs", "p0,external") {
    if (!context.config.otherConfigs.get("enableHiveTest")?.equalsIgnoreCase("true")) {
        logger.info("Hive external tests are disabled")
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String port = context.config.otherConfigs.get("hive2HdfsPort")
    String root = "hdfs://${host}:${port}"
    Path prefix = new Path("/tmp/doris-tvf-merge-${UUID.randomUUID()}")
    Configuration conf = new Configuration()
    conf.setBoolean("fs.hdfs.impl.disable.cache", true)
    FileSystem fs = FileSystem.get(new URI(root), conf, "doris")
    String expected = """(
        SELECT CAST(1 AS BIGINT) AS id, CAST('click' AS STRING) AS event, CAST(NULL AS STRING) AS campaign
        UNION ALL SELECT CAST(2 AS BIGINT), CAST('purchase' AS STRING), CAST('winter' AS STRING)
    ) expected"""
    try {
        for (String format : ["parquet", "orc"]) {
            for (String pair : ["good/old", "good/new", "conflict/old", "conflict/conflict", "complex/complex"]) {
                String fixture = pair.substring(pair.lastIndexOf('/') + 1)
                Path target = new Path(prefix, "${format}/${pair}.${format}")
                fs.mkdirs(target.getParent())
                fs.copyFromLocalFile(new Path(new File(context.dataPath,
                        "test_data/schema_merge/${fixture}.${format}").absolutePath), target)
            }
            String properties = """'format'='${format}', 'merge_schema'='true', 'hadoop.username'='doris'"""
            String goodUri = "${root}${prefix}/${format}/good/*.${format}"
            String source = "HDFS('uri'='${goodUri}', ${properties})"
            String fileSource = "FILE('uri'='${goodUri}', ${properties})"
            quickTest("merge_schema_hdfs_${format}_rows", "SELECT id, event, campaign FROM ${source} ORDER BY id",
                    "SELECT id, event, campaign FROM ${expected} ORDER BY id")
            quickTest("merge_schema_hdfs_${format}_file", "SELECT id, campaign FROM ${fileSource} ORDER BY id",
                    "SELECT id, campaign FROM ${expected} ORDER BY id")
            quickTest("merge_schema_hdfs_${format}_missing_only", "SELECT campaign FROM ${source} ORDER BY campaign",
                    "SELECT campaign FROM ${expected} ORDER BY campaign")
            for (String predicate : ["campaign IS NULL", "campaign = 'winter'"]) {
                quickTest("merge_schema_hdfs_${format}_filter", "SELECT id FROM ${source} WHERE ${predicate} ORDER BY id",
                        "SELECT id FROM ${expected} WHERE ${predicate} ORDER BY id")
            }
            quickTest("merge_schema_hdfs_${format}_counts", "SELECT COUNT(*), COUNT(campaign) FROM ${source}",
                    "SELECT COUNT(*), COUNT(campaign) FROM ${expected}")
            test {
                sql """SELECT * FROM HDFS('uri'='${root}${prefix}/${format}/conflict/*.${format}', ${properties})"""
                exception "Conflicting types"
            }
            test {
                sql """SELECT * FROM HDFS('uri'='${root}${prefix}/${format}/complex/*.${format}', ${properties})"""
                exception "Unsupported schema"
            }
        }
    } finally {
        try {
            if (fs.exists(prefix) && !fs.delete(prefix, true)) {
                throw new IllegalStateException("Failed to remove test directory: ${prefix}")
            }
        } finally {
            fs.close()
        }
    }
}
