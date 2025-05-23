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

import com.google.common.base.Strings;

suite("test_select_into_outfile_with_role") {
    if (Strings.isNullOrEmpty(context.config.awsRoleArn)) {
        logger.info("skip ${name} case, because awsRoleArn is null or empty")
        return
    }

    def endpoint = context.config.awsEndpoint
    def region = context.config.awsRegion
    def bucket = context.config.awsBucket
    def roleArn = context.config.awsRoleArn
    def externalId = context.config.awsExternalId
    def prefix = context.config.awsPrefix

    def randomStr = UUID.randomUUID().toString().replace("-", "")
    def tableName = "test_select_into_outfile_with_role"

    sql """ drop table if exists ${tableName} force;"""
    sql """
        CREATE TABLE ${tableName}
        (
            siteid INT DEFAULT '10',
            citycode SMALLINT NOT NULL,
            username VARCHAR(32) DEFAULT '',
            pv BIGINT SUM DEFAULT '0'
        )
        AGGREGATE KEY(siteid, citycode, username)
        DISTRIBUTED BY HASH(siteid) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
        """

    sql """insert into ${tableName}(siteid, citycode, username, pv) values (1, 1, "xxx", 1),
            (2, 2, "yyy", 2),
            (3, 3, "zzz", 3)
        """
    sql """sync;"""


    sql """
        SELECT * FROM ${tableName}
        INTO OUTFILE "s3://${bucket}/${prefix}/aws_iam_role_p0/test_select_into_outfile_with_role"
        FORMAT AS CSV
        PROPERTIES(
            "s3.endpoint" = "${endpoint}",
            "s3.region" = "${region}",
            "s3.role_arn" = "${roleArn}",
            "s3.external_id"="${externalId}"
        );
    """
}