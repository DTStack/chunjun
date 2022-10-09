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

package com.dtstack.chunjun.connector.test.standalone.ftp;

import com.dtstack.chunjun.connector.containers.ftp.SftpContainer;
import com.dtstack.chunjun.connector.entity.JobAccumulatorResult;
import com.dtstack.chunjun.connector.test.utils.ChunjunFlinkStandaloneTestEnvironment;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.net.URISyntaxException;
import java.time.Duration;
import java.util.stream.Stream;

public class SftpSyncE2eITCase extends ChunjunFlinkStandaloneTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(SftpSyncE2eITCase.class);

    protected static final String sftpImageName = "ftp-e2e-stream";

    protected SftpContainer sftpContainer;

    private void initContainer() throws URISyntaxException {
        sftpContainer = new SftpContainer(sftpImageName);
        sftpContainer
                .withNetwork(NETWORK)
                .withNetworkAliases(sftpImageName)
                .withLogConsumer(new Slf4jLogConsumer(LOG))
                .dependsOn(flinkStandaloneContainer);
    }

    @Before
    public void before() throws Exception {
        super.before();
        LOG.info("Starting sftp containers...");
        initContainer();
        Startables.deepStart(Stream.of(sftpContainer)).join();
        Thread.sleep(5000);
        LOG.info("sftp Containers are started.");
    }

    @After
    public void after() {
        super.after();
        if (sftpContainer != null) {
            sftpContainer.stop();
        }
    }

    @Test
    public void testFtpToStream() throws Exception {
        submitSyncJobOnStandLone(
                ChunjunFlinkStandaloneTestEnvironment.CHUNJUN_HOME
                        + "/chunjun-examples/json/ftp/ftp_stream.json");
        JobAccumulatorResult jobAccumulatorResult = waitUntilJobFinished(Duration.ofMinutes(30));
        Assert.assertEquals(jobAccumulatorResult.getNumRead(), 20);
    }
}
