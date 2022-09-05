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

package com.dtstack.chunjun.connector.containers.postgre;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategyTarget;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.time.Duration;

public class PostgreContainer extends JdbcDatabaseContainer {
    private static final URL POSTGRE_DOCKERFILE =
            PostgreContainer.class.getClassLoader().getResource("docker/postgre/Dockerfile");

    private static final String PG_TEST_USER = "postgre";
    private static final String PG_TEST_PASSWORD = "postgre";
    private static final String PG_TEST_DATABASE = "postgre";
    protected static final String PG_DRIVER_CLASS = "org.postgresql.Driver";
    private static final String POSTGRE_HOST = "chunjun-e2e-postgres";
    public static final Integer POSTGRESQL_PORT = 5432;

    public PostgreContainer() throws URISyntaxException {
        super(
                new ImageFromDockerfile(POSTGRE_HOST, true)
                        .withDockerfile(Paths.get(POSTGRE_DOCKERFILE.toURI())));
        withExposedPorts(POSTGRESQL_PORT);
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
        return PG_DRIVER_CLASS;
    }

    @Override
    public String getJdbcUrl() {
        String additionalUrlParams = this.constructUrlParameters("?", "&");
        return "jdbc:postgresql://"
                + this.getContainerIpAddress()
                + ":"
                + this.getMappedPort(POSTGRESQL_PORT)
                + "/"
                + PG_TEST_DATABASE
                + additionalUrlParams;
    }

    @Override
    public String getUsername() {
        return PG_TEST_USER;
    }

    @Override
    public String getPassword() {
        return PG_TEST_PASSWORD;
    }

    @Override
    protected String getTestQueryString() {
        return "SELECT 1";
    }
}
