/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.chunjun.client.util;

import com.dtstack.chunjun.client.yarn.YarnConfLoader;
import com.dtstack.chunjun.util.ValueUtil;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnConfigOptionsInternal;
import org.apache.flink.yarn.configuration.YarnLogConfigUtil;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

public class YarnSessionClientUtil {

    public static final int MIN_JM_MEMORY = 1024;
    public static final int MIN_TM_MEMORY = 1024;
    public static final String JOBMANAGER_MEMORY_MB = "jobmanager.memory.process.size";
    public static final String TASKMANAGER_MEMORY_MB = "taskmanager.memory.process.size";
    public static final String SLOTS_PER_TASKMANAGER = "taskmanager.slots";

    /**
     * 启动一个flink yarn session
     *
     * @param flinkConfDir flink配置文件路径
     * @param yarnConfDir Hadoop配置文件路径
     * @param flinkLibDir flink lib目录路径
     * @param chunjunPluginDir chunjun插件包路径
     * @param queue yarn队列名称
     * @return
     * @throws Exception
     */
    public static ApplicationId startYarnSession(
            String flinkConfDir,
            String yarnConfDir,
            String flinkLibDir,
            String chunjunPluginDir,
            String queue)
            throws Exception {
        Configuration flinkConfig = GlobalConfiguration.loadConfiguration(flinkConfDir);
        flinkConfig.setString(YarnConfigOptions.APPLICATION_QUEUE, queue);
        flinkConfig.setString(ConfigConstants.PATH_HADOOP_CONFIG, yarnConfDir);
        boolean hdfsPath = false;
        if (StringUtils.isBlank(flinkLibDir)) {
            throw new IllegalArgumentException("The Flink jar path is null");
        } else {
            if (flinkLibDir.startsWith("hdfs://")) {
                hdfsPath = true;
                flinkConfig.set(
                        YarnConfigOptions.PROVIDED_LIB_DIRS,
                        Collections.singletonList(flinkLibDir));
            } else {
                if (!new File(flinkLibDir).exists()) {
                    throw new IllegalArgumentException("The Flink jar path is not exist");
                }
            }
        }
        File log4j =
                new File(flinkConfDir + File.separator + YarnLogConfigUtil.CONFIG_FILE_LOG4J_NAME);
        if (log4j.exists()) {
            flinkConfig.setString(
                    YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE,
                    flinkConfDir + File.separator + YarnLogConfigUtil.CONFIG_FILE_LOG4J_NAME);
        } else {
            File logback =
                    new File(
                            flinkConfDir
                                    + File.separator
                                    + YarnLogConfigUtil.CONFIG_FILE_LOGBACK_NAME);
            if (logback.exists()) {
                flinkConfig.setString(
                        YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE,
                        flinkConfDir + File.separator + YarnLogConfigUtil.CONFIG_FILE_LOGBACK_NAME);
            }
        }

        YarnConfiguration yarnConf = YarnConfLoader.getYarnConf(yarnConfDir);
        yarnConf.set("HADOOP.USER.NAME", "root");
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConf);
        yarnClient.start();

        YarnClusterDescriptor descriptor =
                new YarnClusterDescriptor(
                        flinkConfig,
                        yarnConf,
                        yarnClient,
                        YarnClientYarnClusterInformationRetriever.create(yarnClient),
                        false);

        if (hdfsPath) {
            descriptor.setLocalJarPath(new Path(flinkLibDir + "/flink-dist_2.12-1.12.2.jar"));
        } else {
            List<File> shipFiles = new ArrayList<>();
            File[] jars = new File(flinkLibDir).listFiles();
            if (jars != null) {
                for (File jar : jars) {
                    if (jar.toURI().toURL().toString().contains("flink-dist")) {
                        descriptor.setLocalJarPath(new Path(jar.toURI().toURL().toString()));
                    } else {
                        shipFiles.add(jar);
                    }
                }
            }
            descriptor.addShipFiles(shipFiles);
        }

        File syncFile = new File(chunjunPluginDir);
        List<File> pluginPaths =
                Arrays.stream(Objects.requireNonNull(syncFile.listFiles()))
                        .filter(file -> !file.getName().endsWith("zip"))
                        .collect(Collectors.toList());
        descriptor.addShipFiles(pluginPaths);
        ClusterSpecification clusterSpecification = createClusterSpecification(null);
        ClusterClient<ApplicationId> clusterClient =
                descriptor.deploySessionCluster(clusterSpecification).getClusterClient();
        return clusterClient.getClusterId();
    }

    public static ClusterSpecification createClusterSpecification(Properties conProp) {
        int jobManagerMemoryMb = 1024;
        int taskManagerMemoryMb = 1024;
        int slotsPerTaskManager = 1;

        if (conProp != null) {
            if (conProp.containsKey(JOBMANAGER_MEMORY_MB)) {
                jobManagerMemoryMb =
                        Math.max(
                                MIN_JM_MEMORY,
                                ValueUtil.getInt(conProp.getProperty(JOBMANAGER_MEMORY_MB)));
                jobManagerMemoryMb = jobManagerMemoryMb >> 20;
            }
            if (conProp.containsKey(TASKMANAGER_MEMORY_MB)) {
                taskManagerMemoryMb =
                        Math.max(
                                MIN_TM_MEMORY,
                                ValueUtil.getInt(conProp.getProperty(TASKMANAGER_MEMORY_MB)));
                taskManagerMemoryMb = taskManagerMemoryMb >> 20;
            }
            if (conProp.containsKey(SLOTS_PER_TASKMANAGER)) {
                slotsPerTaskManager = ValueUtil.getInt(conProp.get(SLOTS_PER_TASKMANAGER));
            }
        }

        return new ClusterSpecification.ClusterSpecificationBuilder()
                .setMasterMemoryMB(jobManagerMemoryMb)
                .setTaskManagerMemoryMB(taskManagerMemoryMb)
                .setSlotsPerTaskManager(slotsPerTaskManager)
                .createClusterSpecification();
    }

    public static void main(String[] args) throws Exception {
        String flinkConfDir = "/opt/module/flink-1.12.2/conf";
        String yarnConfDir = "/opt/module/hadoop-2.7.6/etc/hadoop";
        String flinkLibDir = "hdfs://chunjun1:9000/flink-1.12.2/lib";
        String chunjunPluginDir = "/Users/lzq/Desktop/DTStack/chunjun_1.12/chunjun/chunjunplugins/";
        String queue = "c";

        ApplicationId applicationId =
                startYarnSession(flinkConfDir, yarnConfDir, flinkLibDir, chunjunPluginDir, queue);

        System.out.println("clusterId = " + applicationId);
    }
}
