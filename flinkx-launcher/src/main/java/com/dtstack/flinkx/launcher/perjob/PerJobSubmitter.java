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

package com.dtstack.flinkx.launcher.perjob;


import com.dtstack.flinkx.launcher.LauncherOptions;
import com.dtstack.flinkx.util.JsonUtils;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * per job mode submitter
 * Date: 2019/1/7
 * Company: www.dtstack.com
 * @author maqi
 */

public class PerJobSubmitter {


    private static final Logger LOG = LoggerFactory.getLogger(PerJobSubmitter.class);

    public static String submit(LauncherOptions launcherOptions, JobGraph jobGraph) throws Exception {
        Properties confProperties = JsonUtils.jsonStrToObject(launcherOptions.getConfProp(), Properties.class);
        ClusterSpecification clusterSpecification = FLinkPerJobResourceUtil.createClusterSpecification(confProperties);
        PerJobClusterClientBuilder perJobClusterClientBuilder = new PerJobClusterClientBuilder();
        perJobClusterClientBuilder.init(launcherOptions.getYarnconf());

        //flink Jar Path
        String flinkJarPath = launcherOptions.getFlinkLibJar();

        AbstractYarnClusterDescriptor yarnClusterDescriptor = perJobClusterClientBuilder.createPerJobClusterDescriptor(confProperties, flinkJarPath, launcherOptions.getQueue());
        ClusterClient<ApplicationId> clusterClient = yarnClusterDescriptor.deployJobCluster(clusterSpecification,jobGraph,true);
        String applicationId = clusterClient.getClusterId().toString();
        String flinkJobId = jobGraph.getJobID().toString();
        String tips = String.format("deploy per_job with appId: %s, jobId: %s", applicationId, flinkJobId);
        System.out.println(tips);
        LOG.info(tips);
        return applicationId;
    }


}
