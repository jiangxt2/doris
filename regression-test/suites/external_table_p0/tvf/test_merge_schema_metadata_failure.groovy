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

suite("test_merge_schema_metadata_failure", "p0,external") {
    String bucket = getS3BucketName()
    String prefix = "regression/tvf/merge_schema_bad/${UUID.randomUUID()}"
    String resource = "tvf_bad_${UUID.randomUUID().toString().replace('-', '')}"
    String hdfsRoot = "hdfs://${context.config.otherConfigs.get('externalEnvIp')}:" +
            "${context.config.otherConfigs.get('hive2HdfsPort')}"
    Path hdfsPrefix = new Path("/tmp/doris-tvf-bad-${UUID.randomUUID()}")
    boolean enableHive = context.config.otherConfigs.get("enableHiveTest")?.equalsIgnoreCase("true")
    FileSystem fs = null
    def client = getS3Client()
    def keys = []
    boolean resourceCreated = false
    try {
        if (enableHive) {
            Configuration conf = new Configuration()
            conf.setBoolean("fs.hdfs.impl.disable.cache", true)
            fs = FileSystem.get(new URI(hdfsRoot), conf, "doris")
        }
        def statement = context.getConnection().createStatement()
        try {
            statement.execute("""CREATE RESOURCE "${resource}" PROPERTIES (
                "type"="s3", "s3.endpoint"="${getS3Endpoint()}", "s3.region"="${getS3Region()}",
                "s3.access_key"="${getS3AK()}", "s3.secret_key"="${getS3SK()}", "s3.bucket"="${bucket}")""")
            resourceCreated = true
        } finally {
            statement.close()
        }
        for (String format : ["parquet", "orc"]) {
            for (String name : ["old", "corrupt"]) {
                File source = new File(context.dataPath, "test_data/schema_merge/" +
                        (name == "old" ? "old.${format}" : "corrupt.txt"))
                String key = "${prefix}/${format}/${name}.${format}"
                keys.add(key)
                client.putObject(bucket, key, source)
                if (enableHive) {
                    Path target = new Path(hdfsPrefix, "${format}/${name}.${format}")
                    fs.mkdirs(target.getParent())
                    fs.copyFromLocalFile(new Path(source.absolutePath), target)
                }
            }
            test {
                sql """SELECT * FROM S3('uri'='s3://${bucket}/${prefix}/${format}/*.${format}',
                    'format'='${format}', 'resource'='${resource}', 'merge_schema'='true')"""
                exception "Failed to read schema"
            }
            if (enableHive) {
                test {
                    sql """SELECT * FROM HDFS('uri'='${hdfsRoot}${hdfsPrefix}/${format}/*.${format}',
                        'format'='${format}', 'hadoop.username'='doris', 'merge_schema'='true')"""
                    exception "Failed to read schema"
                }
            }
        }
    } finally {
        def failures = []
        keys.each { key ->
            try {
                client.deleteObject(bucket, key)
            } catch (Exception e) {
                failures.add("${key}: ${e.message}")
            }
        }
        try {
            if (fs != null && fs.exists(hdfsPrefix) && !fs.delete(hdfsPrefix, true)) {
                failures.add("Could not remove ${hdfsPrefix}")
            }
        } finally {
            if (fs != null) {
                fs.close()
            }
            if (resourceCreated) {
                sql "DROP RESOURCE '${resource}'"
            }
        }
        if (!failures.isEmpty()) {
            throw new IllegalStateException("Failed to remove test data: ${failures}")
        }
    }
}
