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

package com.dtstack.chunjun.connector.containers.oracle;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategyTarget;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.time.Duration;

public class OracleContainer extends JdbcDatabaseContainer {
    private static final URL ORACLE_DOCKERFILE =
            OracleContainer.class.getClassLoader().getResource("docker/oracle/Dockerfile");

    private static final String ORACLE_HOST = "chunjun-e2e-oracle11";

    private static final String ORACLE_DRIVER_CLASS = "oracle.jdbc.driver.OracleDriver";

    private static final Integer ORACLE_PORT = 1521;

    private static final String SID = "xe";

    private static final String USERNAME = "system";

    private static final String PASSWORD = "oracle";

    public OracleContainer() throws URISyntaxException {
        super(
                new ImageFromDockerfile(ORACLE_HOST, true)
                        .withDockerfile(Paths.get(ORACLE_DOCKERFILE.toURI())));
        withExposedPorts(ORACLE_PORT);
        waitingFor(
                new WaitStrategy() {
                    @Override
                    public void waitUntilReady(WaitStrategyTarget waitStrategyTarget) {}

                    @Override
                    public WaitStrategy withStartupTimeout(Duration startupTimeout) {
                        return null;
                    }
                });
    }

    @Override
    public String getDriverClassName() {
        return ORACLE_DRIVER_CLASS;
    }

    @Override
    public String getJdbcUrl() {
        return "jdbc:oracle:thin:"
                + this.getUsername()
                + "/"
                + this.getPassword()
                + "@"
                + this.getHost()
                + ":"
                + getMappedPort(ORACLE_PORT)
                + ":"
                + this.getSid();
    }

    @Override
    public String getUsername() {
        return USERNAME;
    }

    @Override
    public String getPassword() {
        return PASSWORD;
    }

    @Override
    public String getTestQueryString() {
        return "SELECT 1 FROM DUAL";
    }

    public String getSid() {
        return SID;
    }

    public Integer getOraclePort() {
        return this.getMappedPort(1521);
    }
}
