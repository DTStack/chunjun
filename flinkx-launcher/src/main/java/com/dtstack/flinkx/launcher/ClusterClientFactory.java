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

package com.dtstack.flinkx.launcher;

import com.dtstack.flinkx.enums.ClusterMode;
import com.dtstack.flinkx.options.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.client.deployment.StandaloneClusterDescriptor;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The Factory of ClusterClient
 *
 * Company: www.dtstack.com
 * @author huyifanzju@163.com
 */
public class ClusterClientFactory {

    public static ClusterClient createClusterClient(Options launcherOptions) throws Exception {
        String mode = launcherOptions.getMode();
        if(mode.equals(ClusterMode.standalone.name())) {
            return createStandaloneClient(launcherOptions);
        } else if(mode.equals(ClusterMode.yarn.name())) {
            return createYarnClient(launcherOptions);
        }

        throw new IllegalArgumentException("Unsupported cluster client type: ");
    }

    public static ClusterClient createStandaloneClient(Options launcherOptions) throws Exception {
        Configuration flinkConf = launcherOptions.loadFlinkConfiguration();

        StandaloneClusterDescriptor standaloneClusterDescriptor = new StandaloneClusterDescriptor(flinkConf);
        ClusterClient clusterClient = standaloneClusterDescriptor.retrieve(StandaloneClusterId.getInstance()).getClusterClient();
        return clusterClient;
    }

    public static ClusterClient createYarnClient(Options launcherOptions) {
        Configuration flinkConfig = launcherOptions.loadFlinkConfiguration();
        String yarnConfDir = launcherOptions.getYarnconf();
        if(StringUtils.isNotBlank(yarnConfDir)) {
            try {
                FileSystem.initialize(flinkConfig);

                //进行kerberos验证 如果需要的话
                KerberosInfo kerberosInfo = new KerberosInfo(launcherOptions.getKrb5conf(), launcherOptions.getKeytab(), launcherOptions.getPrincipal(), flinkConfig);
                kerberosInfo.verify();

                YarnConfiguration yarnConf = YarnConfLoader.getYarnConf(yarnConfDir);
                YarnClient yarnClient = YarnClient.createYarnClient();
                yarnClient.init(yarnConf);
                yarnClient.start();
                ApplicationId applicationId;

                if (StringUtils.isEmpty(launcherOptions.getAppId())) {
                    applicationId = getAppIdFromYarn(yarnClient, launcherOptions);
                    if(applicationId == null || StringUtils.isEmpty(applicationId.toString())) {
                        throw new RuntimeException("No flink session found on yarn cluster.");
                    }
                } else {
                    applicationId = ConverterUtils.toApplicationId(launcherOptions.getAppId());
                }

                HighAvailabilityMode highAvailabilityMode = HighAvailabilityMode.fromConfig(flinkConfig);
                if(highAvailabilityMode.equals(HighAvailabilityMode.ZOOKEEPER) && applicationId!=null){
                    flinkConfig.setString(HighAvailabilityOptions.HA_CLUSTER_ID, applicationId.toString());
                }
                YarnClusterDescriptor yarnClusterDescriptor = new YarnClusterDescriptor(
                        flinkConfig,
                        yarnConf,
                        yarnClient,
                        YarnClientYarnClusterInformationRetriever.create(yarnClient),
                        true);
                return yarnClusterDescriptor.retrieve(applicationId).getClusterClient();
            } catch(Exception e) {
                throw new RuntimeException(e);
            }
        }

        throw new UnsupportedOperationException("Haven't been developed yet!");
    }

    private static ApplicationId getAppIdFromYarn(YarnClient yarnClient, Options launcherOptions) throws Exception{
        Set<String> set = new HashSet<>();
        set.add("Apache Flink");
        EnumSet<YarnApplicationState> enumSet = EnumSet.noneOf(YarnApplicationState.class);
        enumSet.add(YarnApplicationState.RUNNING);
        List<ApplicationReport> reportList = yarnClient.getApplications(set, enumSet);

        ApplicationId applicationId = null;
        int maxMemory = -1;
        int maxCores = -1;
        for(ApplicationReport report : reportList) {
            if(!report.getName().startsWith("Flink session")){
                continue;
            }

            if(!report.getYarnApplicationState().equals(YarnApplicationState.RUNNING)) {
                continue;
            }

            if(!report.getQueue().equals(launcherOptions.getQueue())) {
                continue;
            }

            int thisMemory = report.getApplicationResourceUsageReport().getNeededResources().getMemory();
            int thisCores = report.getApplicationResourceUsageReport().getNeededResources().getVirtualCores();

            boolean isOverMaxResource = thisMemory > maxMemory || thisMemory == maxMemory && thisCores > maxCores;
            if(isOverMaxResource) {
                maxMemory = thisMemory;
                maxCores = thisCores;
                applicationId = report.getApplicationId();
            }
        }

        return applicationId;
    }
}
