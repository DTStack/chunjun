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

import com.dtstack.chunjun.connector.containers.mysql.Mysql5Container;
import com.dtstack.chunjun.connector.containers.mysql.MysqlBaseContainer;

import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.util.stream.Stream;

@Slf4j
public class Mysql5SyncE2eITCase extends MysqlBaseSyncE2eITCase {

    protected MysqlBaseContainer mysql5Container;

    @Before
    public void before() throws Exception {
        super.before();
        log.info("Starting mysql5 containers...");
        mysql5Container = new Mysql5Container();
        mysql5Container
                .withNetwork(NETWORK)
                .withNetworkAliases(MYSQL_HOST)
                .withLogConsumer(new Slf4jLogConsumer(log))
                .dependsOn(flinkStandaloneContainer);
        Startables.deepStart(Stream.of(mysql5Container)).join();
        Thread.sleep(5000);
        log.info("Mysql5 Containers are started.");
    }

    @After
    public void after() {
        super.after();
        if (mysql5Container != null) {
            mysql5Container.stop();
        }
    }

    @Test
    public void testMysqlToMysql() throws Exception {
        super.testMysqlToMysql(mysql5Container);
    }
}
