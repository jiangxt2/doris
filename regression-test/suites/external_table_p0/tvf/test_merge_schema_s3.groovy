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

suite("test_merge_schema_s3", "p0,external") {
    String bucket = getS3BucketName()
    String prefix = "regression/tvf/merge_schema/${UUID.randomUUID()}"
    String endpoint = getS3Endpoint()
    String provider = getS3Provider()
    String resource = "tvf_merge_${UUID.randomUUID().toString().replace('-', '')}"
    def client = getS3Client()
    def keys = []
    boolean resourceCreated = false
    String expected = """(
        SELECT CAST(1 AS BIGINT) AS id, CAST('click' AS STRING) AS event, CAST(NULL AS STRING) AS campaign
        UNION ALL SELECT CAST(2 AS BIGINT), CAST('purchase' AS STRING), CAST('winter' AS STRING)
    ) expected"""
    String caseExpected = """(
        SELECT CAST(3 AS BIGINT) AS id, CAST('upper' AS STRING) AS event, CAST('first' AS STRING) AS `ä`
        UNION ALL SELECT CAST(4 AS BIGINT), CAST('lower' AS STRING), CAST('second' AS STRING)
    ) expected_case"""
    try {
        // Execute directly so the framework's SQL logger does not print storage credentials.
        def statement = context.getConnection().createStatement()
        try {
            statement.execute("""CREATE RESOURCE "${resource}" PROPERTIES (
                "type"="s3", "s3.endpoint"="${endpoint}", "s3.region"="${getS3Region()}",
                "s3.access_key"="${getS3AK()}", "s3.secret_key"="${getS3SK()}", "s3.bucket"="${bucket}")""")
            resourceCreated = true
        } finally {
            statement.close()
        }
        for (String format : ["parquet", "orc"]) {
            for (String pair : ["good/old", "good/new", "conflict/old", "conflict/conflict", "complex/complex",
                    "ascii/ascii_upper", "ascii/ascii_lower", "unicode/case_upper", "unicode/case_lower"]) {
                String key = "${prefix}/${format}/${pair}.${format}"
                keys.add(key)
                String fixture = pair.substring(pair.lastIndexOf('/') + 1)
                client.putObject(bucket, key,
                        new File(context.dataPath, "test_data/schema_merge/${fixture}.${format}"))
            }
            String properties = """'format'='${format}', 'merge_schema'='true',
                'provider'='${provider}', 'resource'='${resource}'"""
            String goodUri = "s3://${bucket}/${prefix}/${format}/good/*.${format}"
            String source = "S3('uri'='${goodUri}', ${properties})"
            // FILE resolves the storage provider before its delegate expands the resource.
            String fileSource = """FILE('uri'='${goodUri}', ${properties},
                's3.endpoint'='${endpoint}', 's3.region'='${getS3Region()}')"""
            quickTest("merge_schema_${format}_rows", "SELECT id, event, campaign FROM ${source} ORDER BY id",
                    "SELECT id, event, campaign FROM ${expected} ORDER BY id")
            quickTest("merge_schema_${format}_file", "SELECT id, campaign FROM ${fileSource} ORDER BY id",
                    "SELECT id, campaign FROM ${expected} ORDER BY id")
            quickTest("merge_schema_${format}_missing_only", "SELECT campaign FROM ${source} ORDER BY campaign",
                    "SELECT campaign FROM ${expected} ORDER BY campaign")
            String caseUri = "s3://${bucket}/${prefix}/${format}/ascii/*.${format}"
            quickTest("merge_schema_${format}_ascii_case", "SELECT id, event FROM " +
                    "S3('uri'='${caseUri}', ${properties}) ORDER BY id",
                    "SELECT id, event FROM ${caseExpected} ORDER BY id")
            String unicodePath = "s3://${bucket}/${prefix}/${format}/unicode"
            quickTest("merge_schema_${format}_unicode_lower", "SELECT id, `ä` FROM " +
                    "S3('uri'='${unicodePath}/case_lower.${format}', ${properties})",
                    "SELECT id, `ä` FROM ${caseExpected} WHERE id = 4")
            test {
                sql """SELECT id FROM S3('uri'='${unicodePath}/*.${format}', ${properties})"""
                exception "Unsupported non-ASCII case conversion"
            }
            String defaultProperties = """'format'='${format}', 'merge_schema'='false',
                'provider'='${provider}', 'resource'='${resource}'"""
            quickTest("merge_schema_${format}_disabled", "SELECT id, event FROM " +
                    "S3('uri'='${unicodePath}/case_upper.${format}', ${defaultProperties})",
                    "SELECT id, event FROM ${caseExpected} WHERE id = 3")
            for (String predicate : ["campaign IS NULL", "campaign = 'winter'"]) {
                quickTest("merge_schema_${format}_filter", "SELECT id FROM ${source} WHERE ${predicate} ORDER BY id",
                        "SELECT id FROM ${expected} WHERE ${predicate} ORDER BY id")
            }
            quickTest("merge_schema_${format}_counts", "SELECT COUNT(*), COUNT(campaign) FROM ${source}",
                    "SELECT COUNT(*), COUNT(campaign) FROM ${expected}")
            test {
                sql """SELECT * FROM S3('uri'='s3://${bucket}/${prefix}/${format}/conflict/*.${format}',
                    ${properties})"""
                exception "Conflicting types"
            }
            test {
                sql """SELECT * FROM S3('uri'='s3://${bucket}/${prefix}/${format}/complex/*.${format}',
                    ${properties})"""
                exception "Unsupported schema"
            }
        }
    } finally {
        def failures = []
        try {
            keys.each { key ->
                try {
                    client.deleteObject(bucket, key)
                } catch (Exception e) {
                    failures.add("${key}: ${e.message}")
                }
            }
        } finally {
            if (resourceCreated) {
                sql "DROP RESOURCE '${resource}'"
            }
        }
        if (!failures.isEmpty()) {
            throw new IllegalStateException("Failed to remove test objects: ${failures}")
        }
    }
}
