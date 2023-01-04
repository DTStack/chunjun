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

package com.dtstack.chunjun.connector.test.standalone.postgre.sync;

import com.dtstack.chunjun.connector.containers.postgre.PostgreContainer;
import com.dtstack.chunjun.connector.entity.JobAccumulatorResult;
import com.dtstack.chunjun.connector.test.utils.ChunjunFlinkStandaloneTestEnvironment;
import com.dtstack.chunjun.connector.test.utils.JdbcProxy;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

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

@Slf4j
public class PostgreSyncE2eITCase extends ChunjunFlinkStandaloneTestEnvironment {

    protected static final String POSTGRE_HOST = "chunjun-e2e-postgre";

    private static final URL POSTGRE_INIT_SQL_URL =
            PostgreSyncE2eITCase.class.getClassLoader().getResource("docker/postgre/init.sql");

    public PostgreContainer postgre;

    @Override
    public void before() throws Exception {
        super.before();
        log.info("Starting containers...");
        postgre = new PostgreContainer();
        postgre.withNetwork(NETWORK);
        postgre.withNetworkAliases(POSTGRE_HOST);
        postgre.withLogConsumer(new Slf4jLogConsumer(log));
        Startables.deepStart(Stream.of(postgre)).join();
        Thread.sleep(5000);
        initPostgre();
        log.info("Containers are started.");
    }

    @Override
    public void after() {
        if (postgre != null) {
            postgre.stop();
        }
        super.after();
    }

    @Test
    public void testPostgreToPostgre() throws Exception {
        submitSyncJobOnStandLone(
                ChunjunFlinkStandaloneTestEnvironment.CHUNJUN_HOME
                        + "/chunjun-examples/json/postgresql/postgre_postgre.json");
        JobAccumulatorResult jobAccumulatorResult = waitUntilJobFinished(Duration.ofMinutes(30));

        Assert.assertEquals(jobAccumulatorResult.getNumRead(), 9);
        Assert.assertEquals(jobAccumulatorResult.getNumWrite(), 9);
        JdbcProxy proxy =
                new JdbcProxy(
                        postgre.getJdbcUrl(),
                        postgre.getUsername(),
                        postgre.getPassword(),
                        postgre.getDriverClassName());
        List<String> expectResult =
                Arrays.asList(
                        "101,scooter,Small 2-wheel scooter,3.14",
                        "102,car battery,12V car battery,8.1",
                        "103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8",
                        "104,hammer,12oz carpenter's hammer,0.75",
                        "105,hammer,14oz carpenter's hammer,0.875",
                        "106,hammer,16oz carpenter's hammer,1.0",
                        "107,rocks,box of assorted rocks,5.3",
                        "108,jacket,water resistent black wind breaker,0.1",
                        "109,spare tire,24 inch spare tire,22.2");
        proxy.checkResultWithTimeout(
                expectResult,
                "inventory.products_sink",
                new String[] {"id", "name", "description", "weight"},
                60000L);
    }

    private void initPostgre() throws IOException, SQLException {
        String initSqls =
                FileUtils.readFileToString(new File(POSTGRE_INIT_SQL_URL.getPath()), "UTF-8");
        List<String> executeSqls =
                Arrays.stream(initSqls.split(";"))
                        .filter(sql -> StringUtils.isNotEmpty(StringUtils.strip(sql)))
                        .collect(Collectors.toList());
        try (Connection conn = getPgJdbcConnection();
                Statement statement = conn.createStatement()) {
            for (String sql : executeSqls) {
                statement.execute(sql);
            }
        } catch (SQLException e) {
            log.error("Execute Oracle init sql failed.", e);
            throw e;
        }
    }

    private Connection getPgJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                postgre.getJdbcUrl(), postgre.getUsername(), postgre.getPassword());
    }
}
