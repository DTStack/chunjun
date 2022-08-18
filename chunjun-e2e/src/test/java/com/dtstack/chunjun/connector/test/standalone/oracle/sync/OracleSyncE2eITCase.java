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

package com.dtstack.chunjun.connector.test.standalone.oracle.sync;

import com.dtstack.chunjun.connector.containers.oracle.OracleContainer;
import com.dtstack.chunjun.connector.entity.JobAccumulatorResult;
import com.dtstack.chunjun.connector.test.utils.ChunjunFlinkStandaloneTestEnvironment;
import com.dtstack.chunjun.connector.test.utils.JdbcProxy;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OracleSyncE2eITCase extends ChunjunFlinkStandaloneTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(OracleSyncE2eITCase.class);

    private static final URL ORACLE_INIT_SQL_URL =
            OracleSyncE2eITCase.class.getClassLoader().getResource("docker/oracle/init.sql");

    protected static final String ORACLE_HOST = "chunjun-e2e-oracle";

    public OracleContainer oracle;

    @Override
    public void before() throws Exception {
        super.before();
        LOG.info("Starting containers...");
        oracle = new OracleContainer();
        oracle.withNetwork(NETWORK);
        oracle.withNetworkAliases(ORACLE_HOST);
        oracle.withLogConsumer(new Slf4jLogConsumer(LOG));
        Startables.deepStart(Stream.of(oracle)).join();
        Thread.sleep(5000);
        initOracle();
        LOG.info("Containers are started.");
    }

    @Override
    public void after() {
        if (oracle != null) {
            oracle.stop();
        }
        super.after();
    }

    @Test
    public void testOracleToOracle() throws Exception {
        submitSyncJobOnStandLone(
                ChunjunFlinkStandaloneTestEnvironment.CHUNJUN_HOME
                        + "/chunjun-examples/json/oracle/oracle_oracle.json");
        JobAccumulatorResult jobAccumulatorResult = waitUntilJobFinished(Duration.ofMinutes(30));

        Assert.assertEquals(jobAccumulatorResult.getNumRead(), 10);
        Assert.assertEquals(jobAccumulatorResult.getNumWrite(), 10);

        JdbcProxy proxy =
                new JdbcProxy(
                        oracle.getJdbcUrl(),
                        oracle.getUsername(),
                        oracle.getPassword(),
                        oracle.getDriverClassName());
        List<String> expectResult =
                Arrays.asList(
                        "1,4086.104923538155,2095-02-04 15:59:22.0,2022-08-03 14:11:12.651,FdTY,Abc,Hello",
                        "2,9401.154078754176,1984-10-27 23:04:04.0,2022-08-03 14:11:12.665,kPDM,Abc,Hello",
                        "3,3654.8354065891676,2082-11-01 05:25:45.0,2022-08-03 14:11:12.665,fwhi7A,Abc,Hello",
                        "4,1700.5049489644764,2060-02-01 03:18:48.0,2022-08-03 14:11:12.666,Vam,Abc,Hello",
                        "5,7213.916066384409,2027-11-14 21:55:03.0,2022-08-03 14:11:12.666,X2QZAo,Abc,Hello",
                        "7,7494.472210715716,2096-02-08 06:28:10.0,2022-08-03 14:11:12.668,zW6QXgrz,Abc,Hello",
                        "8,4082.4893142314077,2064-02-09 08:22:15.0,2022-08-03 14:11:12.668,bLLICJ4,Abc,Hello",
                        "9,2248.440916449925,2089-10-14 08:56:57.0,2022-08-03 14:11:12.669,OYB4jD8s,Abc,Hello",
                        "10,1363.0987942903073,1991-11-11 00:46:38.0,2022-08-03 14:11:12.67,NqDOi,Abc,Hello",
                        "11,9036.620205198631,2040-03-20 13:40:13.0,2022-08-03 14:11:12.671,l4bezLJ,Abc,Hello");
        proxy.checkResultWithTimeout(
                expectResult,
                "SYSTEM.TEST_SINK",
                new String[] {
                    "INT_VAL",
                    "DOUBLE_VAL",
                    "DATE_VAL",
                    "TIMESTAMP_VAL",
                    "VAR_VAL",
                    "NAME",
                    "MESSAGE"
                },
                150000L);
    }

    private void initOracle() throws IOException, SQLException {
        String initSqls =
                FileUtils.readFileToString(new File(ORACLE_INIT_SQL_URL.getPath()), "UTF-8");
        List<String> executeSqls =
                Arrays.stream(initSqls.split(";"))
                        .filter(sql -> StringUtils.isNotEmpty(StringUtils.strip(sql)))
                        .collect(Collectors.toList());
        try (Connection conn = getOracleJdbcConnection();
                Statement statement = conn.createStatement()) {
            for (String sql : executeSqls) {
                statement.execute(sql);
            }
        } catch (SQLException e) {
            LOG.error("Execute Oracle init sql failed.", e);
            throw e;
        }
    }

    private Connection getOracleJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                oracle.getJdbcUrl(), oracle.getUsername(), oracle.getPassword());
    }
}
