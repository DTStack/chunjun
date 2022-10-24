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

package com.dtstack.chunjun.connector.containers.mysql;

import com.github.dockerjava.api.command.CreateContainerCmd;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategyTarget;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.function.Consumer;

public class MysqlBaseContainer extends JdbcDatabaseContainer {

    private final String databaseName = "chunjun";
    private final String username = "root";
    private final String password = "123456";

    public static final Integer MYSQL_PORT = 3306;

    public MysqlBaseContainer(String imageName, Path dockerfile) throws URISyntaxException {
        super(new ImageFromDockerfile(imageName, true).withDockerfile(dockerfile));
        withEnv("MYSQL_USER", "admin");
        withEnv("MYSQL_PASSWORD", password);
        withEnv("MYSQL_ROOT_PASSWORD", password);
        withCreateContainerCmdModifier(
                (Consumer<CreateContainerCmd>) cmd -> cmd.withCmd("--lower_case_table_names=1"));
        withExposedPorts(MYSQL_PORT);
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

    public int getDatabasePort() {
        return getMappedPort(MYSQL_PORT);
    }

    @Override
    public String getDriverClassName() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            return "com.mysql.cj.jdbc.Driver";
        } catch (ClassNotFoundException e) {
            return "com.mysql.jdbc.Driver";
        }
    }

    @Override
    public String getJdbcUrl() {
        return getJdbcUrl(databaseName);
    }

    public String getJdbcUrl(String databaseName) {
        String additionalUrlParams = constructUrlParameters("?", "&");
        return "jdbc:mysql://"
                + getHost()
                + ":"
                + getDatabasePort()
                + "/"
                + databaseName
                + additionalUrlParams;
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    protected String getTestQueryString() {
        return "SELECT 1";
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }
}
