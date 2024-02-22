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
package com.dtstack.chunjun.client;

import com.dtstack.chunjun.config.SessionConfig;
import com.dtstack.chunjun.config.YarnAppConfig;
import com.dtstack.chunjun.entry.JobConverter;
import com.dtstack.chunjun.entry.JobDescriptor;
import com.dtstack.chunjun.server.util.JobGraphBuilder;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.CJYarnClusterClientFactory;
import org.apache.flink.yarn.YarnClusterDescriptor;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * 基于yarn 构建client 支持yarn 客户端对flink 任务相关对操作
 *
 * @author xuchao
 * @date 2023-05-22
 */
public class YarnSessionClient implements IClient {

    private static final Logger LOG = LoggerFactory.getLogger(YarnSessionClient.class);

    private ClusterClient client;

    private YarnClient yarnClient;

    private ApplicationId applicationId;

    /** flink-defined configuration */
    protected SessionConfig sessionConfig;

    private JobGraphBuilder jobGraphBuilder;

    public YarnSessionClient(SessionConfig sessionConfig) {
        this.sessionConfig = sessionConfig;
    }

    @Override
    public void open() {
        yarnClient = initYarnClient();
        client = initYarnClusterClient();
        jobGraphBuilder = new JobGraphBuilder(sessionConfig);
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
            client = null;
            LOG.info("close flink yarn session client");
        }

        if (applicationId != null) {
            applicationId = null;
        }

        if (yarnClient != null) {
            yarnClient.close();
            yarnClient = null;
            LOG.info("close yarn session client");
        }
    }

    @Override
    public String getJobLog(String jobId) {
        // 找不到jobId 需要抛出异常
        return null;
    }

    /**
     * flink session 场景下通过restapi 直接获取任务日志
     *
     * @param jobId
     * @return
     */
    @Override
    public String getJobStatus(String jobId) throws Exception {
        Preconditions.checkState(!StringUtils.isEmpty(jobId), "jobId can't be empty!");
        CompletableFuture<JobStatus> jobStatusCompletableFuture =
                client.getJobStatus(JobID.fromHexString(jobId));
        JobStatus jobStatus = jobStatusCompletableFuture.get(20, TimeUnit.SECONDS);
        return jobStatus.toString();
    }

    @Override
    public String getJobStatics(String jobId) {
        // TODO 获取数据同步的同步指标
        // 同步数量
        return null;
    }

    @Override
    public String submitJob(JobDescriptor jobDescriptor) throws Exception {

        // TODO 提交部分需要控制classLoader 的新建，添加cache ，避免生成大量的class 导致metaspace oom.
        JobGraph jobGraph =
                jobGraphBuilder.buildJobGraph(JobConverter.convertJobToArgs(jobDescriptor));
        CompletableFuture<JobID> jobIDCompletableFuture = client.submitJob(jobGraph);
        JobID jobID = jobIDCompletableFuture.get(100, TimeUnit.SECONDS);
        return jobID.toString();
    }

    @Override
    public void cancelJob(String jobId) {}

    public ClusterClient<ApplicationId> initYarnClusterClient() {
        Configuration newConf = new Configuration(sessionConfig.getFlinkConfig());
        applicationId = acquiredAppId();
        if (applicationId == null) {
            // throw new EnginePluginsBaseException("No flink session found on yarn cluster.");
            LOG.warn("No flink session found on yarn cluster, acquireAppId from yarn is null.");
            return null;
        }
        // check is ha enabled

        YarnClusterDescriptor clusterDescriptor =
                getClusterDescriptor(
                        newConf, sessionConfig.getHadoopConfig().getYarnConfiguration());
        ClusterClient<ApplicationId> clusterClient;
        try {
            ClusterClientProvider<ApplicationId> clusterClientProvider =
                    clusterDescriptor.retrieve(applicationId);
            clusterClient = clusterClientProvider.getClusterClient();
        } catch (Exception e) {
            LOG.error("FlinkSession appId {}, but couldn't retrieve Yarn cluster.", applicationId);
            throw new RuntimeException(e);
        }

        LOG.warn("---init flink client with yarn session success----");

        return clusterClient;
    }

    public YarnClusterDescriptor getClusterDescriptor(
            Configuration configuration, YarnConfiguration yarnConfiguration) {
        CJYarnClusterClientFactory yarnClusterClientFactory = new CJYarnClusterClientFactory();

        return yarnClusterClientFactory.createClusterDescriptor(configuration, yarnConfiguration);
    }

    public ApplicationId acquiredAppId() {

        // 根据名称和运行状态获取到指定的applicationId
        Set<String> set = new HashSet<>();
        set.add("Apache Flink");
        EnumSet<YarnApplicationState> enumSet = EnumSet.noneOf(YarnApplicationState.class);
        enumSet.add(YarnApplicationState.RUNNING);
        enumSet.add(YarnApplicationState.ACCEPTED);

        try {
            List<ApplicationReport> reportList = yarnClient.getApplications(set, enumSet);
            for (ApplicationReport report : reportList) {
                YarnApplicationState yarnApplicationState = report.getYarnApplicationState();
                // 注意queue 名称有一些hadoop 分发版本返回的是后缀
                boolean checkQueue =
                        StringUtils.equals(
                                        report.getQueue(), sessionConfig.getAppConfig().getQueue())
                                || StringUtils.startsWith(
                                        sessionConfig.getAppConfig().getQueue(),
                                        "." + report.getQueue());
                boolean checkState = yarnApplicationState.equals(YarnApplicationState.RUNNING);
                boolean checkName =
                        report.getName().equals(sessionConfig.getAppConfig().getApplicationName());

                if (!checkState || !checkQueue || !checkName) {
                    continue;
                }

                return report.getApplicationId();
            }

            // 当前yarn 上没有启动session 的情况
            LOG.info("there are no session on yarn!");
            return null;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private YarnClient initYarnClient() {
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(sessionConfig.getHadoopConfig().getYarnConfiguration());
        yarnClient.start();

        return yarnClient;
    }

    public ClusterClient getClient() {
        return client;
    }

    public void setYarnClient(YarnClient yarnClient) {
        this.yarnClient = yarnClient;
    }

    public YarnClient getYarnClient() {
        return yarnClient;
    }

    public YarnApplicationState getCurrentSessionStatus() throws IOException, YarnException {
        if (yarnClient == null || applicationId == null) {
            throw new RuntimeException("yarnClient or applicationId is null.");
        }

        return yarnClient.getApplicationReport(applicationId).getYarnApplicationState();
    }

    public static void main(String[] args) throws IOException {
        YarnAppConfig yarnAppConfig = new YarnAppConfig();
        yarnAppConfig.setApplicationName("dt_xc");
        yarnAppConfig.setQueue("default");

        String flinkConfDir = "/Users/xuchao/conf/flinkconf/dev_conf_local/";
        String hadoopConfDir = "/Users/xuchao/conf/hadoopconf/dev_hadoop_flink01";
        String flinkLibDir = "/Users/xuchao/MyPrograms/Flink/flink-1.12.7/lib";
        String chunJunLibDir = "/Users/xuchao/IdeaProjects/chunjun/chunjun-dist";

        SessionConfig sessionConfig =
                new SessionConfig(flinkConfDir, hadoopConfDir, flinkLibDir, chunJunLibDir);
        sessionConfig.setAppConfig(yarnAppConfig);
        sessionConfig.loadFlinkConfiguration();
        sessionConfig.loadHadoopConfiguration();

        YarnSessionClient yarnSessionClient = new YarnSessionClient(sessionConfig);
        yarnSessionClient.open();
        System.out.println("-------");
        yarnSessionClient.close();
    }
}
