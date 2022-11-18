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
package com.dtstack.chunjun.client.yarn;

import com.dtstack.chunjun.client.ClusterClientHelper;
import com.dtstack.chunjun.client.JobDeployer;
import com.dtstack.chunjun.client.util.JobGraphUtil;
import com.dtstack.chunjun.options.Options;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterDescriptor;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.yarn.configuration.YarnConfigOptions.APPLICATION_ID;
import static org.apache.flink.yarn.configuration.YarnConfigOptions.APPLICATION_QUEUE;

@Slf4j
public class YarnSessionClusterClientHelper implements ClusterClientHelper<ApplicationId> {

    @Override
    public ClusterClient<ApplicationId> submit(JobDeployer jobDeployer) throws Exception {
        Options launcherOptions = jobDeployer.getLauncherOptions();
        List<String> programArgs = jobDeployer.getProgramArgs();

        Configuration flinkConfig = launcherOptions.loadFlinkConfiguration();
        String yarnConfDir = launcherOptions.getHadoopConfDir();

        try {
            YarnConfiguration yarnConf = YarnConfLoader.getYarnConf(yarnConfDir);

            SecurityUtils.install(new SecurityConfiguration(flinkConfig));
            return SecurityUtils.getInstalledContext()
                    .runSecured(
                            () -> {
                                FileSystem.initialize(flinkConfig, null);
                                try (YarnClient yarnClient = YarnClient.createYarnClient()) {
                                    yarnClient.init(yarnConf);
                                    yarnClient.start();
                                    ApplicationId applicationId;

                                    if (StringUtils.isEmpty(flinkConfig.get(APPLICATION_ID))) {
                                        applicationId = getAppIdFromYarn(yarnClient, flinkConfig);
                                        if (applicationId == null
                                                || StringUtils.isEmpty(applicationId.toString())) {
                                            throw new RuntimeException(
                                                    "No flink session found on yarn cluster.");
                                        }
                                    } else {
                                        applicationId =
                                                ApplicationId.fromString(
                                                        flinkConfig.get(APPLICATION_ID));
                                    }

                                    HighAvailabilityMode highAvailabilityMode =
                                            HighAvailabilityMode.fromConfig(flinkConfig);
                                    if (highAvailabilityMode.equals(HighAvailabilityMode.ZOOKEEPER)
                                            && applicationId != null) {
                                        flinkConfig.setString(
                                                HighAvailabilityOptions.HA_CLUSTER_ID,
                                                applicationId.toString());
                                    }
                                    try (YarnClusterDescriptor yarnClusterDescriptor =
                                            new YarnClusterDescriptor(
                                                    flinkConfig,
                                                    yarnConf,
                                                    yarnClient,
                                                    YarnClientYarnClusterInformationRetriever
                                                            .create(yarnClient),
                                                    true)) {
                                        ClusterClient<ApplicationId> clusterClient =
                                                yarnClusterDescriptor
                                                        .retrieve(applicationId)
                                                        .getClusterClient();
                                        JobGraph jobGraph =
                                                JobGraphUtil.buildJobGraph(
                                                        launcherOptions,
                                                        programArgs.toArray(new String[0]));
                                        jobGraph.getClasspaths().clear();
                                        jobGraph.getUserJars().clear();
                                        jobGraph.getUserArtifacts().clear();
                                        JobID jobID = clusterClient.submitJob(jobGraph).get();
                                        log.info("submit job successfully, jobID = {}", jobID);
                                        return clusterClient;
                                    }
                                }
                            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ApplicationId getAppIdFromYarn(YarnClient yarnClient, Configuration flinkConfig)
            throws Exception {
        Set<String> set = new HashSet<>();
        set.add("Apache Flink");
        EnumSet<YarnApplicationState> enumSet = EnumSet.noneOf(YarnApplicationState.class);
        enumSet.add(YarnApplicationState.RUNNING);
        List<ApplicationReport> reportList = yarnClient.getApplications(set, enumSet);

        ApplicationId applicationId = null;
        long maxMemory = -1;
        int maxCores = -1;
        for (ApplicationReport report : reportList) {
            if (!report.getName().startsWith("Flink session")) {
                continue;
            }

            if (!report.getYarnApplicationState().equals(YarnApplicationState.RUNNING)) {
                continue;
            }

            if (!report.getQueue().equals(flinkConfig.get(APPLICATION_QUEUE))) {
                continue;
            }

            long thisMemory =
                    report.getApplicationResourceUsageReport().getNeededResources().getMemorySize();
            int thisCores =
                    report.getApplicationResourceUsageReport()
                            .getNeededResources()
                            .getVirtualCores();

            boolean isOverMaxResource =
                    thisMemory > maxMemory || thisMemory == maxMemory && thisCores > maxCores;
            if (isOverMaxResource) {
                maxMemory = thisMemory;
                maxCores = thisCores;
                applicationId = report.getApplicationId();
            }
        }

        return applicationId;
    }
}
