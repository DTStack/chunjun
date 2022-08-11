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

package com.dtstack.chunjun.connector.test;

import com.dtstack.chunjun.client.Launcher;
import com.dtstack.chunjun.connector.test.containers.FlinkStandaloneContainer;
import com.dtstack.chunjun.connector.test.entity.JobAccumulatorResult;
import com.dtstack.chunjun.connector.test.entity.LaunchCommandBuilder;
import com.dtstack.chunjun.enums.ClusterMode;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.table.api.ValidationException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategyTarget;
import org.testcontainers.lifecycle.Startables;

import javax.annotation.Nullable;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.dtstack.chunjun.connector.test.containers.FlinkStandaloneContainer.JOB_MANAGER_REST_PORT;
import static com.dtstack.chunjun.connector.test.containers.FlinkStandaloneContainer.JOB_MANAGER_RPC_PORT;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * @author jayce
 * @version 1.0
 * @date 2022/8/11 11:18
 */
public class ChunjunFlinkStandaloneE2eTest {
    private static final Logger LOG = LoggerFactory.getLogger(ChunjunBaseE2eTest.class);

    public static final URL FLINK_CONF_DIR_URL =
            ChunjunBaseE2eTest.class.getClassLoader().getResource("docker/flink/standalone");

    final public static String CHUNJUN_DIST = new File(System.getProperty("user.dir")).getParentFile().getAbsolutePath() + File.separator + "chunjun-dist";

    private static final String FLINK_STANDALONE_HOST = "standalone";

    @ClassRule
    public static final Network NETWORK = Network.newNetwork();

    protected FlinkStandaloneContainer flinkStandaloneContainer;

    @Nullable
    private RestClusterClient<StandaloneClusterId> restClusterClient;

    @Before
    public void before() throws URISyntaxException {
        LOG.info("Starting flink standalone containers...");

        flinkStandaloneContainer = new FlinkStandaloneContainer(FLINK_STANDALONE_HOST)
//                .withCommand(FLINK_STANDALONE_HOST)
                .withNetwork(NETWORK)
                .withNetworkAliases(FLINK_STANDALONE_HOST)
                .withExposedPorts(JOB_MANAGER_REST_PORT, JOB_MANAGER_RPC_PORT)
                .withFileSystemBind("D:\\code\\chunjun\\chunjun-dist", "/code/chunjun/chunjun-dist")
                .withLogConsumer(new Slf4jLogConsumer(LOG))
                .withEnv("CHUNJUN_HOME","/code/chunjun/chunjun-dist")
                .withEnv("FLINK_HOME","/opt/flink/conf")
                .waitingFor(new WaitStrategy() {
                    @Override
                    public void waitUntilReady(WaitStrategyTarget waitStrategyTarget) {

                    }

                    @Override
                    public WaitStrategy withStartupTimeout(Duration startupTimeout) {
                        return null;
                    }
                });
        ;
        Startables.deepStart(Stream.of(flinkStandaloneContainer)).join();
        LOG.info("Containers are started.");
    }

    @After
    public void after() {
        if (restClusterClient != null) {
            restClusterClient.close();
        }

        if (flinkStandaloneContainer != null) {
            flinkStandaloneContainer.stop();
        }
    }

    public RestClusterClient<StandaloneClusterId> getRestClusterClient() {
        if (restClusterClient != null) {
            return restClusterClient;
        }
        checkState(
                flinkStandaloneContainer.isRunning(),
                "Cluster client should only be retrieved for a running cluster");
        try {
            final Configuration clientConfiguration = new Configuration();
            clientConfiguration.set(RestOptions.ADDRESS, flinkStandaloneContainer.getHost());
            clientConfiguration.set(
                    RestOptions.PORT, flinkStandaloneContainer.getMappedPort(JOB_MANAGER_REST_PORT));
            this.restClusterClient =
                    new RestClusterClient<>(clientConfiguration, StandaloneClusterId.getInstance());
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to create client for Flink container cluster", e);
        }
        return restClusterClient;
    }


    protected void submitSyncJobOnStandLone(String syncConf) throws Exception {
        HashMap<String, Object> customProperties = Maps.newHashMap();
        customProperties.put("jobmanager.rpc.port", flinkStandaloneContainer.getMappedPort(JOB_MANAGER_RPC_PORT));
        customProperties.put("rest.port", flinkStandaloneContainer.getMappedPort(JOB_MANAGER_REST_PORT));

        String[] syncs =
                new LaunchCommandBuilder("sync")
                        .withFlinkConfDir("D:\\code\\chunjun\\chunjun-e2e\\target\\test-classes\\docker\\flink\\standalone")
                        .withRunningMode(ClusterMode.standalone)
                        .withJobContentPath(syncConf)
                        .withChunJunDistDir("/code/chunjun/chunjun-dist")
                        .withFlinkCustomConf(customProperties)
                        .builder();
        Launcher.main(syncs);
    }

    public JobAccumulatorResult waitUntilJobFinished(Duration timeout)
            throws ExecutionException, InterruptedException {
        RestClusterClient<?> clusterClient = getRestClusterClient();
        Deadline deadline = Deadline.fromNow(timeout);
        int i = 0;
        while (deadline.hasTimeLeft()) {
            Collection<JobStatusMessage> jobStatusMessages;
            try {
                jobStatusMessages = clusterClient.listJobs().get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                if (i++ > 10) {
                    throw new RuntimeException("Error when fetching job status.", e);
                }
                Thread.sleep(5000L);
                LOG.warn("Error when fetching job status.", e);
                continue;
            }

            if (jobStatusMessages != null && !jobStatusMessages.isEmpty()) {
                JobStatusMessage message = jobStatusMessages.iterator().next();
                JobStatus jobStatus = message.getJobState();
                if (jobStatus.isTerminalState()) {
                    if (message.getJobState().equals(JobStatus.FINISHED)) {
                        CompletableFuture<Map<String, Object>> accumulators =
                                clusterClient.getAccumulators(message.getJobId());
                        Map<String, Object> data = accumulators.get();
                        return printResult(data);
                    }
                    throw new ValidationException(
                            String.format(
                                    "Job has been terminated! JobName: %s, JobID: %s, Status: %s",
                                    message.getJobName(),
                                    message.getJobId(),
                                    message.getJobState()));
                } else if (jobStatus == JobStatus.FINISHED) {
                    CompletableFuture<Map<String, Object>> accumulators =
                            clusterClient.getAccumulators(message.getJobId());
                    Map<String, Object> data = accumulators.get();
                    return printResult(data);
                }
            }
        }
        throw new RuntimeException("wait job finished timeout");
    }

    public JobAccumulatorResult printResult(Map<String, Object> result) {
        List<String> names = Lists.newArrayList();
        List<String> values = Lists.newArrayList();
        result.forEach(
                (name, val) -> {
                    names.add(name);
                    values.add(String.valueOf(val));
                });

        int maxLength = 0;
        for (String name : names) {
            maxLength = Math.max(maxLength, name.length());
        }
        maxLength += 5;

        StringBuilder builder = new StringBuilder(128);
        builder.append("\n*********************************************\n");
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            builder.append(name + StringUtils.repeat(" ", maxLength - name.length()));
            builder.append("|  ").append(values.get(i));

            if (i + 1 < names.size()) {
                builder.append("\n");
            }
        }
        builder.append("\n*********************************************\n");
        LOG.info(builder.toString());

        return GsonUtil.GSON.fromJson(GsonUtil.GSON.toJson(result), JobAccumulatorResult.class);
    }
}
