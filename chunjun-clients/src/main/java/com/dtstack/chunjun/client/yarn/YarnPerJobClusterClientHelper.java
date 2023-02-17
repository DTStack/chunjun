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
import com.dtstack.chunjun.client.util.PluginInfoUtil;
import com.dtstack.chunjun.options.Options;
import com.dtstack.chunjun.util.MapUtil;
import com.dtstack.chunjun.util.ValueUtil;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnConfigOptionsInternal;
import org.apache.flink.yarn.configuration.YarnLogConfigUtil;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.configuration.TaskManagerOptions.NUM_TASK_SLOTS;

@Slf4j
public class YarnPerJobClusterClientHelper implements ClusterClientHelper<ApplicationId> {

    public static final int MIN_JM_MEMORY = 1024;
    public static final int MIN_TM_MEMORY = 1024;

    @Override
    public ClusterClient<ApplicationId> submit(JobDeployer jobDeployer) throws Exception {
        Options launcherOptions = jobDeployer.getLauncherOptions();
        String confProp = launcherOptions.getConfProp();
        if (StringUtils.isBlank(confProp)) {
            throw new IllegalArgumentException("per-job mode must have confProp!");
        }
        String libJar = launcherOptions.getFlinkLibDir();
        if (StringUtils.isBlank(libJar)) {
            throw new IllegalArgumentException("per-job mode must have flink lib path!");
        }

        Configuration flinkConfig = jobDeployer.getEffectiveConfiguration();

        SecurityUtils.install(new SecurityConfiguration(flinkConfig));

        ClusterSpecification clusterSpecification = createClusterSpecification(jobDeployer);

        try (YarnClusterDescriptor descriptor =
                createPerJobClusterDescriptor(launcherOptions, flinkConfig)) {
            ClusterClientProvider<ApplicationId> provider =
                    descriptor.deployJobCluster(
                            clusterSpecification, new JobGraph("chunjun"), true);
            String applicationId = provider.getClusterClient().getClusterId().toString();
            String flinkJobId = clusterSpecification.getJobGraph().getJobID().toString();
            log.info("deploy per_job with appId: {}}, jobId: {}", applicationId, flinkJobId);

            return provider.getClusterClient();
        }
    }

    private YarnClusterDescriptor createPerJobClusterDescriptor(
            Options launcherOptions, Configuration flinkConfig) throws MalformedURLException {
        String flinkLibDir = launcherOptions.getFlinkLibDir();
        String flinkConfDir = launcherOptions.getFlinkConfDir();

        if (StringUtils.isBlank(flinkLibDir)) {
            throw new IllegalArgumentException("The Flink lib dir is null");
        }

        File log4jPath =
                new File(flinkConfDir + File.separator + YarnLogConfigUtil.CONFIG_FILE_LOG4J_NAME);
        if (log4jPath.exists()) {
            flinkConfig.setString(
                    YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE,
                    log4jPath.getAbsolutePath());
        } else {
            File logbackPath =
                    new File(
                            flinkConfDir
                                    + File.separator
                                    + YarnLogConfigUtil.CONFIG_FILE_LOGBACK_NAME);
            if (logbackPath.exists()) {
                flinkConfig.setString(
                        YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE,
                        logbackPath.getAbsolutePath());
            }
        }

        YarnConfiguration yarnConfig =
                YarnConfLoader.getYarnConf(launcherOptions.getHadoopConfDir());
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConfig);
        yarnClient.start();

        YarnClusterDescriptor descriptor =
                new YarnClusterDescriptor(
                        flinkConfig,
                        yarnConfig,
                        yarnClient,
                        YarnClientYarnClusterInformationRetriever.create(yarnClient),
                        false);

        if (!new File(flinkLibDir).exists()) {
            throw new IllegalArgumentException("The Flink lib dir is not exist");
        }

        boolean isRemoteJarPath =
                !CollectionUtil.isNullOrEmpty(flinkConfig.get(YarnConfigOptions.PROVIDED_LIB_DIRS));
        List<File> shipFiles = new ArrayList<>();
        File[] jars = new File(flinkLibDir).listFiles();
        if (jars != null) {
            for (File jar : jars) {
                if (jar.toURI().toURL().toString().contains("flink-dist")) {
                    descriptor.setLocalJarPath(new Path(jar.toURI().toURL().toString()));
                } else if (!isRemoteJarPath) {
                    shipFiles.add(jar);
                }
            }
        }
        descriptor.addShipFiles(shipFiles);

        return descriptor;
    }

    private ClusterSpecification createClusterSpecification(JobDeployer jobDeployer)
            throws IOException {
        Options launcherOptions = jobDeployer.getLauncherOptions();
        List<String> programArgs = jobDeployer.getProgramArgs();

        Properties conProp =
                MapUtil.jsonStrToObject(launcherOptions.getConfProp(), Properties.class);
        int jobManagerMemoryMb = 1024;
        int taskManagerMemoryMb = 1024;
        int slotsPerTaskManager = 1;

        if (conProp != null) {
            if (conProp.containsKey(JobManagerOptions.TOTAL_PROCESS_MEMORY.key())) {
                jobManagerMemoryMb =
                        Math.max(
                                MIN_JM_MEMORY,
                                MemorySize.parse(
                                                conProp.getProperty(
                                                        JobManagerOptions.TOTAL_PROCESS_MEMORY
                                                                .key()))
                                        .getMebiBytes());
            }
            if (conProp.containsKey(TaskManagerOptions.TOTAL_PROCESS_MEMORY.key())) {
                taskManagerMemoryMb =
                        Math.max(
                                MIN_TM_MEMORY,
                                MemorySize.parse(
                                                conProp.getProperty(
                                                        TaskManagerOptions.TOTAL_PROCESS_MEMORY
                                                                .key()))
                                        .getMebiBytes());
            }
            if (conProp.containsKey(NUM_TASK_SLOTS.key())) {
                slotsPerTaskManager = ValueUtil.getInt(conProp.get(NUM_TASK_SLOTS.key()));
            }
        }

        ClusterSpecification clusterSpecification =
                new ClusterSpecification.ClusterSpecificationBuilder()
                        .setMasterMemoryMB(jobManagerMemoryMb)
                        .setTaskManagerMemoryMB(taskManagerMemoryMb)
                        .setSlotsPerTaskManager(slotsPerTaskManager)
                        .createClusterSpecification();

        // 设置从savepoint启动
        if (conProp != null && conProp.containsKey("execution.savepoint.path")) {
            clusterSpecification.setSpSetting(
                    SavepointRestoreSettings.forPath(
                            ValueUtil.getStringVal(conProp.get("execution.savepoint.path"))));
        }

        clusterSpecification.setCreateProgramDelay(true);

        String pluginRoot = launcherOptions.getChunjunDistDir();
        String coreJarPath = PluginInfoUtil.getCoreJarPath(pluginRoot);
        File jarFile = new File(coreJarPath);
        clusterSpecification.setConfiguration(launcherOptions.loadFlinkConfiguration());
        clusterSpecification.setClasspaths(Collections.emptyList());
        clusterSpecification.setEntryPointClass(PluginInfoUtil.getMainClass());
        clusterSpecification.setJarFile(jarFile);

        clusterSpecification.setProgramArgs(programArgs.toArray(new String[0]));
        clusterSpecification.setCreateProgramDelay(true);
        clusterSpecification.setYarnConfiguration(
                YarnConfLoader.getYarnConf(launcherOptions.getHadoopConfDir()));

        return clusterSpecification;
    }
}
