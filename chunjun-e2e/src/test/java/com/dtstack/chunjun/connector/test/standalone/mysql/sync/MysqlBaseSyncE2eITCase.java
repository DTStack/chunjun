/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.connector.test.standalone.mysql.sync;

import com.dtstack.chunjun.connector.containers.mysql.MysqlBaseContainer;
import com.dtstack.chunjun.connector.entity.JobAccumulatorResult;
import com.dtstack.chunjun.connector.test.utils.ChunjunFlinkStandaloneTestEnvironment;
import com.dtstack.chunjun.connector.test.utils.JdbcProxy;

import org.junit.Assert;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

public class MysqlBaseSyncE2eITCase extends ChunjunFlinkStandaloneTestEnvironment {

    protected static final String MYSQL_HOST = "chunjun-e2e-mysql";

    public void testMysqlToMysql(MysqlBaseContainer genericContainer) throws Exception {
        submitSyncJobOnStandLone(
                ChunjunFlinkStandaloneTestEnvironment.CHUNJUN_HOME
                        + "/chunjun-examples/json/mysql/mysql_mysql_batch.json");
        JobAccumulatorResult jobAccumulatorResult = waitUntilJobFinished(Duration.ofMinutes(30));

        Assert.assertEquals(jobAccumulatorResult.getNumRead(), 12);
        Assert.assertEquals(jobAccumulatorResult.getNumWrite(), 12);

        String jdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s?useSSL=false&allowPublicKeyRetrieval=true",
                        genericContainer.getHost(),
                        genericContainer.getDatabasePort(),
                        genericContainer.getDatabaseName());

        // assert final results
        JdbcProxy proxy =
                new JdbcProxy(
                        jdbcUrl,
                        genericContainer.getUsername(),
                        genericContainer.getPassword(),
                        genericContainer.getDriverClassName());
        List<String> expectResult =
                Arrays.asList(
                        "1,zhang1,1,2022-08-12,13022539328,zhang3@chunjun,1,2022-08-12 11:20:15.0,1,1,1,1.00,1.0,1.0,1",
                        "10,zhang10,10,2022-08-12,13022539328,zhang3@chunjun,1,2022-08-12 11:20:15.0,1,1,1,1.00,1.0,1.0,1",
                        "11,zhang11,11,2022-08-12,13022539328,zhang3@chunjun,1,2022-08-12 11:20:15.0,1,1,1,1.00,1.0,1.0,1",
                        "12,zhang12,12,2022-08-12,13022539328,zhang3@chunjun,1,2022-08-12 11:20:15.0,1,1,1,1.00,1.0,1.0,1",
                        "2,zhang2,2,2022-08-12,13022539328,zhang3@chunjun,1,2022-08-12 11:20:15.0,1,1,1,1.00,1.0,1.0,1",
                        "3,zhang3,3,2022-08-12,13022539328,zhang3@chunjun,1,2022-08-12 11:20:15.0,1,1,1,1.00,1.0,1.0,1",
                        "4,zhang4,4,2022-08-12,13022539328,zhang3@chunjun,1,2022-08-12 11:20:15.0,1,1,1,1.00,1.0,1.0,1",
                        "5,zhang5,5,2022-08-12,13022539328,zhang3@chunjun,1,2022-08-12 11:20:15.0,1,1,1,1.00,1.0,1.0,1",
                        "6,zhang6,6,2022-08-12,13022539328,zhang3@chunjun,1,2022-08-12 11:20:15.0,1,1,1,1.00,1.0,1.0,1",
                        "7,zhang7,7,2022-08-12,13022539328,zhang3@chunjun,1,2022-08-12 11:20:15.0,1,1,1,1.00,1.0,1.0,1",
                        "8,zhang8,8,2022-08-12,13022539328,zhang3@chunjun,1,2022-08-12 11:20:15.0,1,1,1,1.00,1.0,1.0,1",
                        "9,zhang9,9,2022-08-12,13022539328,zhang3@chunjun,1,2022-08-12 11:20:15.0,1,1,1,1.00,1.0,1.0,1");
        proxy.checkResultWithTimeout(
                expectResult,
                "test_sink",
                new String[] {
                    "id",
                    "name",
                    "idcard",
                    "birthday",
                    "mobile",
                    "email",
                    "gender",
                    "create_time",
                    "type_smallint",
                    "type_mediumint",
                    "type_bigint",
                    "type_decimal",
                    "type_float",
                    "type_double",
                    "type_text"
                },
                60000L);
    }
}
