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

package com.dtstack.chunjun.environment;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The LocalStreamEnvironment is a StreamExecutionEnvironment that runs the program locally,
 * multi-threaded, in the JVM where the environment is instantiated. It spawns an embedded Flink
 * cluster in the background and executes the program on that cluster.
 *
 * <p>When this environment is instantiated, it uses a default parallelism of {@code 1}. The default
 * parallelism can be set via {@link #setParallelism(int)}.
 */
@Slf4j
public class MyLocalStreamEnvironment extends StreamExecutionEnvironment {

    private final Configuration configuration;

    private final List<URL> classpath = Collections.emptyList();

    private SavepointRestoreSettings settings;

    public void setSettings(SavepointRestoreSettings settings) {
        this.settings = settings;
    }

    /** Creates a new mini cluster stream environment that uses the default configuration. */
    public MyLocalStreamEnvironment() {
        this(new Configuration());
    }

    /**
     * Creates a new mini cluster stream environment that configures its local executor with the
     * given configuration.
     *
     * @param configuration The configuration used to configure the local executor.
     */
    public MyLocalStreamEnvironment(@Nonnull Configuration configuration) {
        super(validateAndGetConfiguration(configuration));
        if (!ExecutionEnvironment.areExplicitEnvironmentsAllowed()) {
            throw new InvalidProgramException(
                    "The LocalStreamEnvironment cannot be used when submitting a program through a client, "
                            + "or running in a TestEnvironment context.");
        }
        this.configuration = configuration;
        this.setParallelism(1);
    }

    private static Configuration validateAndGetConfiguration(final Configuration configuration) {
        if (!ExecutionEnvironment.areExplicitEnvironmentsAllowed()) {
            throw new InvalidProgramException(
                    "The LocalStreamEnvironment cannot be used when submitting a program through a client, "
                            + "or running in a TestEnvironment context.");
        }
        final Configuration effectiveConfiguration = new Configuration(checkNotNull(configuration));
        effectiveConfiguration.set(DeploymentOptions.TARGET, "local");
        effectiveConfiguration.set(DeploymentOptions.ATTACHED, true);
        return effectiveConfiguration;
    }

    private void clearFlinkLocalDistributedCache(JobGraph jobGraph) throws IOException {
        File osTmpDir = new File(System.getProperty("java.io.tmpdir"));
        JobID jobID = jobGraph.getJobID();
        File[] flinkTmpDirs =
                osTmpDir.listFiles(
                        (dir, name) -> name.startsWith("flink-distributed-cache-" + jobID));

        if (flinkTmpDirs != null) {
            for (File flinkTmpDir : flinkTmpDirs) {
                if (flinkTmpDir.exists() && flinkTmpDir.isDirectory()) {
                    FileUtils.deleteDirectory(flinkTmpDir);
                }
            }
        }
    }

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    /**
     * Executes the JobGraph of the on a mini cluster of CLusterUtil with a user specified name.
     *
     * @param jobName name of the job
     * @return The result of the job execution, containing elapsed time and accumulators.
     */
    @Override
    public JobExecutionResult execute(String jobName) throws Exception {
        // transform the streaming program into a JobGraph
        StreamGraph streamGraph = getStreamGraph();
        streamGraph.setJobName(jobName);

        JobGraph jobGraph = streamGraph.getJobGraph();
        jobGraph.setClasspaths(classpath);

        if (settings != null) {
            jobGraph.setSavepointRestoreSettings(settings);
        }

        Configuration configuration = new Configuration();
        configuration.addAll(jobGraph.getJobConfiguration());
        configuration.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE.key(), "512M");
        configuration.setInteger(
                TaskManagerOptions.NUM_TASK_SLOTS, jobGraph.getMaximumParallelism());

        // add (and override) the settings with what the user defined
        configuration.addAll(this.configuration);

        if (!configuration.contains(RestOptions.BIND_PORT)) {
            configuration.setString(RestOptions.BIND_PORT, "0");
        }

        int numSlotsPerTaskManager =
                configuration.getInteger(
                        TaskManagerOptions.NUM_TASK_SLOTS, jobGraph.getMaximumParallelism());

        MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .setConfiguration(configuration)
                        .setNumSlotsPerTaskManager(numSlotsPerTaskManager)
                        .build();

        if (log.isInfoEnabled()) {
            log.info("Running job on local embedded Flink mini cluster");
        }

        try (MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();
            configuration.setInteger(
                    RestOptions.PORT, miniCluster.getRestAddress().get().getPort());

            return miniCluster.executeJobBlocking(jobGraph);
        } finally {
            transformations.clear();
            clearFlinkLocalDistributedCache(jobGraph);
        }
    }
}
