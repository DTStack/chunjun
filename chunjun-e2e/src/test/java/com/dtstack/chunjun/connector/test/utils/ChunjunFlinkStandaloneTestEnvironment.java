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

package com.dtstack.chunjun.connector.test.utils;

import com.dtstack.chunjun.connector.containers.flink.FlinkStandaloneContainer;
import com.dtstack.chunjun.connector.entity.JobAccumulatorResult;
import com.dtstack.chunjun.connector.entity.LaunchCommandBuilder;
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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

import javax.annotation.Nullable;

import java.io.File;
import java.net.URL;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkState;

@Slf4j
public class ChunjunFlinkStandaloneTestEnvironment {

    public static final String CHUNJUN_HOME =
            new File(System.getProperty("user.dir")).getParentFile().getAbsolutePath();

    public static final URL FLINK_CONF_DIR_URL =
            ChunjunFlinkStandaloneTestEnvironment.class
                    .getClassLoader()
                    .getResource("docker/flink/standalone");

    public static final String CHUNJUN_DIST =
            new File(System.getProperty("user.dir")).getParentFile().getAbsolutePath()
                    + "/chunjun-dist";

    public static final String CHUNJUN_LIB =
            new File(System.getProperty("user.dir")).getParentFile().getAbsolutePath() + "/lib";

    public static final String CHUNJUN_BIN =
            new File(System.getProperty("user.dir")).getParentFile().getAbsolutePath() + "/bin";

    public static final String CHUNJUN_EXAMPLES =
            new File(System.getProperty("user.dir")).getParentFile().getAbsolutePath()
                    + "/chunjun-examples";

    private static final String FLINK_STANDALONE_HOST = "standalone";

    public static final int JOB_MANAGER_REST_PORT = 8081;

    public static final int JOB_MANAGER_RPC_PORT = 6213;

    @ClassRule public static final Network NETWORK = Network.newNetwork();

    protected FlinkStandaloneContainer flinkStandaloneContainer;

    @Nullable private RestClusterClient<StandaloneClusterId> restClusterClient;

    @Before
    public void before() throws Exception {
        Assert.assertTrue("chunjun-dist directory must exists", new File(CHUNJUN_DIST).exists());

        log.info("Starting flink standalone containers...");

        flinkStandaloneContainer =
                new FlinkStandaloneContainer(FLINK_STANDALONE_HOST)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(FLINK_STANDALONE_HOST)
                        .withExposedPorts(JOB_MANAGER_REST_PORT, JOB_MANAGER_RPC_PORT)
                        .withFileSystemBind(CHUNJUN_DIST, CHUNJUN_DIST)
                        .withFileSystemBind(CHUNJUN_LIB, CHUNJUN_LIB)
                        .withFileSystemBind(CHUNJUN_EXAMPLES, CHUNJUN_EXAMPLES)
                        .withLogConsumer(new Slf4jLogConsumer(log));
        Startables.deepStart(Stream.of(flinkStandaloneContainer)).join();
        flinkStandaloneContainer.copyFileToContainer(
                MountableFile.forHostPath(CHUNJUN_BIN + "/submit.sh"), CHUNJUN_BIN + "/submit.sh");
        log.info("Containers are started.");
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
                    RestOptions.PORT,
                    flinkStandaloneContainer.getMappedPort(JOB_MANAGER_REST_PORT));
            this.restClusterClient =
                    new RestClusterClient<>(clientConfiguration, StandaloneClusterId.getInstance());
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to create client for Flink container cluster", e);
        }
        return restClusterClient;
    }

    protected void submitSyncJobOnStandLone(String syncConfig) throws Exception {
        this.submitSyncJobOnStandLoneWithParameters(syncConfig, null);
    }

    protected void submitSyncJobOnStandLoneWithParameters(
            String syncConfig, Map<String, String> parameters) throws Exception {
        String[] syncs =
                new LaunchCommandBuilder("sync")
                        .withFlinkConfDir("/opt/flink/conf")
                        .withRunningMode(ClusterMode.standalone)
                        .withJobContentPath(syncConfig)
                        .withChunJunDistDir(CHUNJUN_DIST)
                        .withFlinkLibDir(CHUNJUN_LIB)
                        .withParameters(parameters)
                        .builder();
        Container.ExecResult execResult =
                flinkStandaloneContainer.execInContainer(
                        "bash", CHUNJUN_BIN + "/submit.sh", String.join(" ", syncs));
        log.info(execResult.getStdout());
        log.error(execResult.getStderr());
        if (execResult.getExitCode() != 0) {
            throw new AssertionError("Failed when submitting the SQL job.");
        }
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
                log.warn("Error when fetching job status.", e);
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

    public void waitUntilJobRunning(Duration timeout) {
        RestClusterClient<?> clusterClient = getRestClusterClient();
        Deadline deadline = Deadline.fromNow(timeout);
        while (deadline.hasTimeLeft()) {
            Collection<JobStatusMessage> jobStatusMessages;
            try {
                jobStatusMessages = clusterClient.listJobs().get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.warn("Error when fetching job status.", e);
                continue;
            }
            if (jobStatusMessages != null && !jobStatusMessages.isEmpty()) {
                JobStatusMessage message = jobStatusMessages.iterator().next();
                JobStatus jobStatus = message.getJobState();
                if (jobStatus.isTerminalState()) {
                    throw new ValidationException(
                            String.format(
                                    "Job has been terminated! JobName: %s, JobID: %s, Status: %s",
                                    message.getJobName(),
                                    message.getJobId(),
                                    message.getJobState()));
                } else if (jobStatus == JobStatus.RUNNING) {
                    return;
                }
            }
        }
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
        log.info(builder.toString());

        return GsonUtil.GSON.fromJson(GsonUtil.GSON.toJson(result), JobAccumulatorResult.class);
    }
}
