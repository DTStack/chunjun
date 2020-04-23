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

package com.dtstack.flinkx.launcher.perJob;

import com.dtstack.flinkx.options.Options;
import com.dtstack.flinkx.util.MapUtil;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Date: 2019/09/11
 * Company: www.dtstack.com
 * @author tudou
 */
public class PerJobSubmitter {
    private static final Logger LOG = LoggerFactory.getLogger(PerJobSubmitter.class);

    /**
     * submit per-job task
     * @param options LauncherOptions
     * @param jobGraph JobGraph
     * @return
     * @throws Exception
     */
    public static String submit(Options options, JobGraph jobGraph) throws Exception{
        LOG.info("start to submit per-job task, options = {}", options.toString());
        Properties conProp = MapUtil.jsonStrToObject(options.getConfProp(), Properties.class);
        ClusterSpecification clusterSpecification = FlinkPerJobResourceUtil.createClusterSpecification(conProp);
        PerJobClusterClientBuilder perJobClusterClientBuilder = new PerJobClusterClientBuilder();
        perJobClusterClientBuilder.init(options, conProp);

        YarnClusterDescriptor descriptor = perJobClusterClientBuilder.createPerJobClusterDescriptor(conProp, options, jobGraph);
        ClusterClientProvider<ApplicationId> provider = descriptor.deployJobCluster(clusterSpecification, jobGraph, true);
        String applicationId = provider.getClusterClient().getClusterId().toString();
        String flinkJobId = jobGraph.getJobID().toString();
        LOG.info("deploy per_job with appId: {}}, jobId: {}", applicationId, flinkJobId);
        return applicationId;
    }
}