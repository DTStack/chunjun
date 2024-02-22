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
package com.dtstack.chunjun.server;

import com.dtstack.chunjun.config.SessionConfig;
import com.dtstack.chunjun.config.YarnAppConfig;
import com.dtstack.chunjun.server.util.FileUtil;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.yarn.CJYarnClusterClientFactory;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 启动一个session
 *
 * @author xuchao
 * @date 2023-06-28
 */
public class SessionDeployer {

    private static final Logger LOG = LoggerFactory.getLogger(SessionDeployer.PREFIX_HDFS);

    public static final String PREFIX_HDFS = "hdfs://";

    public static final String REMOTE_FLINK_LIB_DIR = "remoteFlinkLibDir";

    public static final String REMOTE_CHUNJUN_LIB_DIR = "remoteChunJunLibDir";

    public static final String FLINK_LIB_DIR = "flinkLibDir";

    public static final String CHUNJUN_LIB_DIR = "chunjunLibDir";

    private String flinkLibDir;

    private String chunJunLibDir;

    private String flinkConfPath;

    private String hadoopConfDir;

    private boolean flinkHighAvailability;

    private YarnConfiguration yarnConf;

    /** flink-defined configuration */
    protected Configuration flinkConfiguration;

    private YarnAppConfig yarnAppConfig;

    private ClusterSpecification clusterSpecification;

    private SessionStatusInfo sessionStatus;

    public SessionDeployer(SessionConfig sessionConfig, SessionStatusInfo sessionStatusInfo) {
        this.yarnAppConfig = sessionConfig.getAppConfig();
        this.flinkLibDir = sessionConfig.getFlinkLibDir();
        this.flinkConfPath = sessionConfig.getFlinkConfDir();
        this.chunJunLibDir = sessionConfig.getChunJunLibDir();
        this.hadoopConfDir = sessionConfig.getHadoopConfDir();

        this.flinkConfiguration = sessionConfig.getFlinkConfig();
        this.yarnConf = sessionConfig.getHadoopConfig().getYarnConfiguration();
        this.sessionStatus = sessionStatusInfo;
    }

    public void doDeploy() {

        ClusterSpecification.ClusterSpecificationBuilder builder =
                new ClusterSpecification.ClusterSpecificationBuilder();

        MemorySize taskManagerMB = flinkConfiguration.get(TaskManagerOptions.TOTAL_PROCESS_MEMORY);
        MemorySize jobManagerMB = flinkConfiguration.get(JobManagerOptions.TOTAL_PROCESS_MEMORY);
        Integer numTaskSlots = flinkConfiguration.get(TaskManagerOptions.NUM_TASK_SLOTS);

        builder.setMasterMemoryMB(jobManagerMB.getMebiBytes());
        builder.setTaskManagerMemoryMB(taskManagerMB.getMebiBytes());
        builder.setSlotsPerTaskManager(numTaskSlots);
        clusterSpecification = builder.createClusterSpecification();

        try (YarnClusterDescriptor yarnSessionDescriptor = createYarnClusterDescriptor()) {
            ClusterClient<ApplicationId> clusterClient =
                    yarnSessionDescriptor
                            .deploySessionCluster(clusterSpecification)
                            .getClusterClient();
            LOG.info("start session with cluster id :" + clusterClient.getClusterId().toString());
            // 重新开始session check
            sessionStatus.setStatus(ESessionStatus.HEALTHY);
        } catch (Throwable e) {
            LOG.error("Couldn't deploy Yarn session cluster, ", e);
            // 重新开始session check
            sessionStatus.setStatus(ESessionStatus.UNHEALTHY);
            throw new RuntimeException(e);
        }
    }

    public YarnClusterDescriptor createYarnClusterDescriptor() {
        Configuration flinkCopyConf = new Configuration(flinkConfiguration);
        FileUtil.checkFileExist(flinkLibDir);

        if (!flinkHighAvailability) {
            setNoneHaModeConfig(flinkCopyConf);
        } else {
            // 由engine管控的yarnsession clusterId不进行设置，默认使用appId作为clusterId
            flinkCopyConf.removeConfig(HighAvailabilityOptions.HA_CLUSTER_ID);
        }

        setHdfsFlinkJarPath(flinkCopyConf);
        flinkCopyConf.setString(
                YarnConfigOptions.APPLICATION_QUEUE.key(), yarnAppConfig.getQueue());
        flinkCopyConf.set(DeploymentOptions.TARGET, YarnDeploymentTarget.SESSION.getName());
        CJYarnClusterClientFactory clusterClientFactory = new CJYarnClusterClientFactory();

        YarnConfiguration newYarnConfig = new YarnConfiguration();
        for (Map.Entry<String, String> next : yarnConf) {
            newYarnConfig.set(next.getKey(), next.getValue());
        }

        return clusterClientFactory.createClusterDescriptor(flinkCopyConf, newYarnConfig);
    }

    public void setNoneHaModeConfig(Configuration configuration) {
        configuration.setString(
                HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.NONE.toString());
        configuration.removeConfig(HighAvailabilityOptions.HA_CLUSTER_ID);
        configuration.removeConfig(HighAvailabilityOptions.HA_ZOOKEEPER_ROOT);
        configuration.removeConfig(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM);
    }

    /** 插件包及Lib包提前上传至HDFS，设置远程HDFS路径参数 */
    // TODO
    public void setHdfsFlinkJarPath(Configuration flinkConfiguration) {
        // 检查HDFS上是否已经上传插件包及Lib包
        String remoteFlinkLibDir = null;
        // remotePluginRootDir默认不为空
        String remoteChunjunLibDir = null;

        // 不考虑二者只有其一上传到了hdfs上的情况
        if (StringUtils.startsWith(remoteFlinkLibDir, PREFIX_HDFS)
                && StringUtils.startsWith(remoteChunjunLibDir, PREFIX_HDFS)) {
            flinkConfiguration.setString(REMOTE_FLINK_LIB_DIR, remoteFlinkLibDir);
            flinkConfiguration.setString(REMOTE_CHUNJUN_LIB_DIR, remoteChunjunLibDir);
            flinkConfiguration.setString(FLINK_LIB_DIR, flinkLibDir);
            flinkConfiguration.setString(CHUNJUN_LIB_DIR, chunJunLibDir);
        }
    }
}
