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

import com.dtstack.chunjun.connector.containers.mysql.Mysql8Container;
import com.dtstack.chunjun.connector.containers.mysql.MysqlBaseContainer;

import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.util.stream.Stream;

@Slf4j
public class Mysql8SyncE2eITCase extends MysqlBaseSyncE2eITCase {

    protected MysqlBaseContainer mysql8Container;

    @Before
    public void before() throws Exception {
        super.before();
        log.info("Starting mysql8 containers...");
        mysql8Container = new Mysql8Container();
        mysql8Container
                .withNetwork(NETWORK)
                .withNetworkAliases(MYSQL_HOST)
                .withLogConsumer(new Slf4jLogConsumer(log))
                .dependsOn(flinkStandaloneContainer);
        Startables.deepStart(Stream.of(mysql8Container)).join();
        Thread.sleep(5000);
        log.info("Mysql8 Containers are started.");
    }

    @After
    public void after() {
        super.after();
        if (mysql8Container != null) {
            mysql8Container.stop();
        }
    }

    @Test
    public void testMysqlToMysql() throws Exception {
        super.testMysqlToMysql(mysql8Container);
    }
}
