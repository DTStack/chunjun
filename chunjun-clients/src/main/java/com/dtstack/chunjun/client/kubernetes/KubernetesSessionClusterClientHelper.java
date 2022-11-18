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
package com.dtstack.chunjun.client.kubernetes;

import com.dtstack.chunjun.client.ClusterClientHelper;
import com.dtstack.chunjun.client.JobDeployer;
import com.dtstack.chunjun.client.util.JobGraphUtil;
import com.dtstack.chunjun.options.Options;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.KubernetesClusterDescriptor;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClientFactory;
import org.apache.flink.runtime.jobgraph.JobGraph;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

@Slf4j
public class KubernetesSessionClusterClientHelper implements ClusterClientHelper<String> {

    @Override
    public ClusterClient<String> submit(JobDeployer jobDeployer) throws Exception {
        Options launcherOptions = jobDeployer.getLauncherOptions();
        List<String> programArgs = jobDeployer.getProgramArgs();

        Configuration configuration = jobDeployer.getEffectiveConfiguration();

        String clusterId = configuration.get(KubernetesConfigOptions.CLUSTER_ID);
        if (StringUtils.isBlank(clusterId)) {
            throw new IllegalArgumentException("Kubernetes Session Mode Must Set CLUSTER_ID!");
        }

        FlinkKubeClient flinkKubeClient =
                FlinkKubeClientFactory.getInstance().fromConfiguration(configuration, "ChunJun");
        try (KubernetesClusterDescriptor descriptor =
                new KubernetesClusterDescriptor(configuration, flinkKubeClient)) {
            ClusterClientProvider<String> retrieve = descriptor.retrieve(clusterId);
            ClusterClient<String> clusterClient = retrieve.getClusterClient();

            JobGraph jobGraph =
                    JobGraphUtil.buildJobGraph(launcherOptions, programArgs.toArray(new String[0]));
            JobID jobID = clusterClient.submitJob(jobGraph).get();
            log.info("submit job successfully, jobID = {}", jobID);
            return clusterClient;
        }
    }
}
