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

package com.dtstack.chunjun.connector.test.standalone.emqx;

import com.dtstack.chunjun.connector.containers.emqx.EmqxContainer;
import com.dtstack.chunjun.connector.entity.JobAccumulatorResult;
import com.dtstack.chunjun.connector.test.utils.ChunjunFlinkStandaloneTestEnvironment;

import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.net.URISyntaxException;
import java.time.Duration;
import java.util.stream.Stream;

@Slf4j
public class EmqxSyncE2eITCase extends ChunjunFlinkStandaloneTestEnvironment {

    protected static final String emqxImageName = "emqx-e2e-stream";

    protected EmqxContainer emqxContainer;

    private void initContainer() throws URISyntaxException {
        emqxContainer = new EmqxContainer(emqxImageName);
        emqxContainer
                .withNetwork(NETWORK)
                .withNetworkAliases(emqxImageName)
                .withLogConsumer(new Slf4jLogConsumer(log))
                .dependsOn(flinkStandaloneContainer);
    }

    @Before
    public void before() throws Exception {
        super.before();
        log.info("Starting emqx containers...");
        initContainer();
        Startables.deepStart(Stream.of(emqxContainer)).join();
        Thread.sleep(5000);
        log.info("emqx Containers are started.");
    }

    @After
    public void after() {
        super.after();
        if (emqxContainer != null) {
            emqxContainer.stop();
        }
    }

    @Test
    public void testStreamToEmqx() throws Exception {
        submitSyncJobOnStandLone(
                ChunjunFlinkStandaloneTestEnvironment.CHUNJUN_HOME
                        + "/chunjun-examples/json/emqx/stream_emqx.json");
        JobAccumulatorResult jobAccumulatorResult = waitUntilJobFinished(Duration.ofMinutes(30));
        Assert.assertEquals(jobAccumulatorResult.getNumRead(), 30);
    }
}
