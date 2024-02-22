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
package org.apache.flink.yarn;

import com.dtstack.chunjun.config.ChunJunServerOptions;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 扩展YarnClusterDescriptor 提供ChunJun 包上传功能
 *
 * @author xuchao
 * @date 2023-09-13
 */
public class CJYarnClusterDescriptor extends YarnClusterDescriptor {

    private static final Logger LOG = LoggerFactory.getLogger(CJYarnClusterDescriptor.class);

    private final YarnConfiguration yarnConfiguration;

    private final Configuration flinkConfiguration;

    public CJYarnClusterDescriptor(
            Configuration flinkConfiguration,
            YarnConfiguration yarnConfiguration,
            YarnClient yarnClient,
            YarnClusterInformationRetriever yarnClusterInformationRetriever,
            boolean sharedYarnClient) {
        super(
                flinkConfiguration,
                yarnConfiguration,
                yarnClient,
                yarnClusterInformationRetriever,
                sharedYarnClient);
        this.flinkConfiguration = flinkConfiguration;
        this.yarnConfiguration = yarnConfiguration;
    }

    @Override
    public ApplicationReport startAppMaster(
            Configuration configuration,
            String applicationName,
            String yarnClusterEntrypoint,
            JobGraph jobGraph,
            YarnClient yarnClient,
            YarnClientApplication yarnApplication,
            ClusterSpecification clusterSpecification)
            throws Exception {
        // 只上传ChunJun 包到hdfs 作为资源目录，不添加到 classpath ,防止chunjun 扩展对connector 对 flink
        // 包的影响，比如guava版本不一致，导致类冲突
        // 只在启动session 的情况下主动上传ChunJun 包
        if (jobGraph == null) {
            LOG.info("start to upload chunjun dir.");
            uploadCJDir(configuration, yarnApplication);
            LOG.info("upload chunjun dir end.");
        }
        return super.startAppMaster(
                configuration,
                applicationName,
                yarnClusterEntrypoint,
                jobGraph,
                yarnClient,
                yarnApplication,
                clusterSpecification);
    }

    public void uploadCJDir(Configuration configuration, YarnClientApplication yarnApplication)
            throws Exception {
        FileSystem fs = FileSystem.get(yarnConfiguration);

        ApplicationSubmissionContext appContext = yarnApplication.getApplicationSubmissionContext();
        Path stagingDirPath = getStagingDir(fs);
        FileSystem stagingDirFs = stagingDirPath.getFileSystem(yarnConfiguration);
        final List<Path> providedLibDirs =
                Utils.getQualifiedRemoteProvidedLibDirs(configuration, yarnConfiguration);

        YarnApplicationFileUploader fileUploader =
                YarnApplicationFileUploader.from(
                        stagingDirFs,
                        stagingDirPath,
                        providedLibDirs,
                        appContext.getApplicationId(),
                        getFileReplication());

        String path = configuration.get(ChunJunServerOptions.CHUNJUN_DIST_PATH);
        Path chunjunDistPath = new Path(path);
        Collection<Path> shipFiles = new ArrayList<>();
        shipFiles.add(chunjunDistPath);
        shipFiles =
                shipFiles.stream()
                        .filter(filePath -> !filePath.toString().contains("/server/"))
                        .collect(Collectors.toList());

        fileUploader.registerMultipleLocalResources(
                shipFiles, Path.CUR_DIR, LocalResourceType.FILE);
    }

    private int getFileReplication() {
        final int yarnFileReplication =
                yarnConfiguration.getInt(
                        DFSConfigKeys.DFS_REPLICATION_KEY, DFSConfigKeys.DFS_REPLICATION_DEFAULT);
        final int fileReplication =
                flinkConfiguration.getInteger(YarnConfigOptions.FILE_REPLICATION);
        return fileReplication > 0 ? fileReplication : yarnFileReplication;
    }
}
