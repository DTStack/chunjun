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
package com.dtstack.flinkx.client.yarn;

import com.dtstack.flinkx.client.ClusterClientHelper;
import com.dtstack.flinkx.client.JobDeployer;

import com.dtstack.flinkx.client.KerberosInfo;
import com.dtstack.flinkx.client.util.PluginInfoUtil;
import com.dtstack.flinkx.options.Options;
import com.dtstack.flinkx.util.MapUtil;
import com.dtstack.flinkx.util.ValueUtil;
import org.apache.commons.lang3.StringUtils;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;

import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;

import org.apache.flink.util.CollectionUtil;
import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnConfigOptionsInternal;
import org.apache.flink.yarn.configuration.YarnLogConfigUtil;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;


/**
 * @program: flinkx
 * @author: xiuzhu
 * @create: 2021/05/31
 */
public class YarnPerJobClusterClientHelper implements ClusterClientHelper {

    private static final Logger LOG = LoggerFactory.getLogger(YarnPerJobClusterClientHelper.class);

    public final static int MIN_JM_MEMORY = 1024;
    public final static int MIN_TM_MEMORY = 1024;
    public final static String JOBMANAGER_MEMORY_MB = "jobmanager.memory.process.size";
    public final static String TASKMANAGER_MEMORY_MB = "taskmanager.memory.process.size";
    public final static String SLOTS_PER_TASKMANAGER = "taskmanager.slots";

    @Override
    public ClusterClient submit(JobDeployer jobDeployer) throws Exception {
        Options launcherOptions = jobDeployer.getLauncherOptions();
        String confProp = launcherOptions.getConfProp();
        if (StringUtils.isBlank(confProp)) {
            throw new IllegalArgumentException("per-job mode must have confProp!");
        }
        String libJar = launcherOptions.getFlinkLibJar();
        if (StringUtils.isBlank(libJar)) {
            throw new IllegalArgumentException("per-job mode must have flink lib path!");
        }

        Configuration flinkConfig = jobDeployer.getEffectiveConfiguration();

        KerberosInfo kerberosInfo = new KerberosInfo(launcherOptions.getKrb5conf(),launcherOptions.getKeytab(),launcherOptions.getPrincipal(), flinkConfig);
        kerberosInfo.verify();
        SecurityUtils.install(new SecurityConfiguration(flinkConfig));

        ClusterSpecification clusterSpecification = createClusterSpecification(jobDeployer);
        YarnClusterDescriptor descriptor = createPerJobClusterDescriptor(launcherOptions, flinkConfig);

        ClusterClientProvider<ApplicationId> provider = descriptor.deployJobCluster(clusterSpecification, new JobGraph(), true);
        String applicationId = provider.getClusterClient().getClusterId().toString();
        String flinkJobId = clusterSpecification.getJobGraph().getJobID().toString();
        LOG.info("deploy per_job with appId: {}}, jobId: {}", applicationId, flinkJobId);

        return provider.getClusterClient();
    }

    private YarnClusterDescriptor createPerJobClusterDescriptor(Options launcherOptions, Configuration flinkConfig) throws MalformedURLException {
        String flinkJarPath = launcherOptions.getFlinkLibJar();
        String flinkConfDir = launcherOptions.getFlinkconf();

        if (StringUtils.isBlank(flinkJarPath)) {
            throw new IllegalArgumentException("The Flink jar path is null");
        }

        File log4jPath = new File(flinkConfDir + File.separator + YarnLogConfigUtil.CONFIG_FILE_LOG4J_NAME);
        if (log4jPath.exists()) {
            flinkConfig.setString(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE, log4jPath.getAbsolutePath());
        } else {
            File logbackPath = new File(flinkConfDir + File.separator + YarnLogConfigUtil.CONFIG_FILE_LOGBACK_NAME);
            if (logbackPath.exists()) {
                flinkConfig.setString(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE, logbackPath.getAbsolutePath());
            }

        }

        YarnConfiguration yarnConfig = YarnConfLoader.getYarnConf(launcherOptions.getYarnconf());
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConfig);
        yarnClient.start();

        YarnClusterDescriptor descriptor = new YarnClusterDescriptor(
                flinkConfig,
                yarnConfig,
                yarnClient,
                YarnClientYarnClusterInformationRetriever.create(yarnClient),
                false);

        if (!new File(flinkJarPath).exists()) {
            throw new IllegalArgumentException("The Flink jar path is not exist");
        }

        boolean isRemoteJarPath = !CollectionUtil.isNullOrEmpty(flinkConfig.get(YarnConfigOptions.PROVIDED_LIB_DIRS));
        List<File> shipFiles = new ArrayList<>();
        File[] jars = new File(flinkJarPath).listFiles();
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

    private ClusterSpecification createClusterSpecification(JobDeployer jobDeployer) throws IOException {
        Options launcherOptions = jobDeployer.getLauncherOptions();
        List<String> programArgs = jobDeployer.getProgramArgs();

        Properties conProp = MapUtil.jsonStrToObject(launcherOptions.getConfProp(), Properties.class);
        int jobManagerMemoryMb = 1024;
        int taskManagerMemoryMb = 1024;
        int slotsPerTaskManager = 1;

        if (conProp != null) {
            if (conProp.containsKey(JOBMANAGER_MEMORY_MB)) {
                jobManagerMemoryMb = Math.max(MIN_JM_MEMORY, ValueUtil.getInt(conProp.getProperty(JOBMANAGER_MEMORY_MB)));
            }
            if (conProp.containsKey(TASKMANAGER_MEMORY_MB)) {
                taskManagerMemoryMb = Math.max(MIN_TM_MEMORY, ValueUtil.getInt(conProp.getProperty(TASKMANAGER_MEMORY_MB)));
            }
            if (conProp.containsKey(SLOTS_PER_TASKMANAGER)) {
                slotsPerTaskManager = ValueUtil.getInt(conProp.get(SLOTS_PER_TASKMANAGER));
            }
        }

        ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
                .setMasterMemoryMB(jobManagerMemoryMb)
                .setTaskManagerMemoryMB(taskManagerMemoryMb)
                .setSlotsPerTaskManager(slotsPerTaskManager)
                .createClusterSpecification();

        clusterSpecification.setCreateProgramDelay(true);

        String pluginRoot = launcherOptions.getPluginRoot();
        String coreJarPath = PluginInfoUtil.getCoreJarPath(pluginRoot);
        File jarFile = new File(coreJarPath);
        clusterSpecification.setConfiguration(launcherOptions.loadFlinkConfiguration());
        clusterSpecification.setClasspaths(Collections.emptyList());
        clusterSpecification.setEntryPointClass(PluginInfoUtil.getMainClass());
        clusterSpecification.setJarFile(jarFile);

        clusterSpecification.setProgramArgs(programArgs.toArray(new String[0]));
        clusterSpecification.setCreateProgramDelay(true);
        clusterSpecification.setYarnConfiguration(YarnConfLoader.getYarnConf(launcherOptions.getYarnconf()));

        return clusterSpecification;
    }
}
