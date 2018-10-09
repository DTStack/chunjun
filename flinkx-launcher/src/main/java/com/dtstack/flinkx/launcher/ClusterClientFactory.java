/**
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

import org.apache.commons.lang.StringUtils;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.StandaloneClusterDescriptor;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.StandaloneClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterClient;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import java.lang.reflect.Field;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The Factory of ClusterClient
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class ClusterClientFactory {

    public static ClusterClient createClusterClient(LauncherOptions launcherOptions) {
        String clientType = launcherOptions.getMode();
        if(ClusterMode.standalone.name().equals(clientType)) {
            return createStandaloneClient(launcherOptions);
        } else if(ClusterMode.yarn.name().equals(clientType)) {
            return createYarnClient(launcherOptions);
        }else if(ClusterMode.yarnPer.name().equals(clientType)){//start yarn-session
            return createPerYarnClient(launcherOptions);
        }
        throw new IllegalArgumentException("Unsupported cluster client type: ");
    }

    private static StandaloneClusterClient createStandaloneClient(LauncherOptions launcherOptions) {
        String flinkConfDir = launcherOptions.getFlinkconf();
        Configuration config = GlobalConfiguration.loadConfiguration(flinkConfDir);
        StandaloneClusterDescriptor descriptor = new StandaloneClusterDescriptor(config);
        StandaloneClusterClient clusterClient = descriptor.retrieve(null);
        clusterClient.setDetached(true);
        return clusterClient;
    }

    private static YarnClusterClient createPerYarnClient(LauncherOptions launcherOptions) {
        YarnClusterClient cluster = null;
        try {
            Configuration flinkConf = FlinkUtil.getFlinkConfiguration(launcherOptions.getFlinkconf());
            ClusterSpecification clusterSpecification = FlinkUtil.createDefaultClusterSpecification(flinkConf,launcherOptions.getPriority());
            AbstractYarnClusterDescriptor descriptor = FlinkUtil.createPerJobClusterDescriptor(flinkConf,FlinkUtil.getYarnConfiguration(flinkConf,launcherOptions.getYarnconf()),launcherOptions.getFlinkLibJar(),launcherOptions.getQueue());
            cluster = descriptor.deploySessionCluster(clusterSpecification);
        } catch (Exception e){
            throw new RuntimeException("Couldn't deploy Yarn session cluster" + e.getMessage());
        }
        return cluster;
    }

        private static YarnClusterClient createYarnClient(LauncherOptions launcherOptions) {
        Configuration config = FlinkUtil.getFlinkConfiguration(launcherOptions.getFlinkconf());
        String yarnConfDir = launcherOptions.getYarnconf();
        if(StringUtils.isNotBlank(yarnConfDir)) {
            try {
                    org.apache.hadoop.conf.Configuration yarnConf = FlinkUtil.getYarnConfiguration(config,yarnConfDir);
                    YarnClient yarnClient = YarnClient.createYarnClient();
                    yarnClient.init(yarnConf);
                    yarnClient.start();
                    String applicationId = null;

                    Set<String> set = new HashSet<>();
                    set.add("Apache Flink");
                    EnumSet<YarnApplicationState> enumSet = EnumSet.noneOf(YarnApplicationState.class);
                    enumSet.add(YarnApplicationState.RUNNING);
                    List<ApplicationReport> reportList = yarnClient.getApplications(set, enumSet);

                    int maxMemory = -1;
                    int maxCores = -1;
                    for(ApplicationReport report : reportList) {
                        if(!report.getName().startsWith("Flink session")){
                            continue;
                        }

                        if(!report.getYarnApplicationState().equals(YarnApplicationState.RUNNING)) {
                            continue;
                        }

                        int thisMemory = report.getApplicationResourceUsageReport().getNeededResources().getMemory();
                        int thisCores = report.getApplicationResourceUsageReport().getNeededResources().getVirtualCores();
                        if(thisMemory > maxMemory || thisMemory == maxMemory && thisCores > maxCores) {
                            maxMemory = thisMemory;
                            maxCores = thisCores;
                            applicationId = report.getApplicationId().toString();
                        }

                    }

                    if(org.apache.commons.lang3.StringUtils.isEmpty(applicationId)) {
                        throw new RuntimeException("No flink session found on yarn cluster.");
                    }

                    yarnClient.stop();

                    AbstractYarnClusterDescriptor clusterDescriptor = new YarnClusterDescriptor(config, ".");
                    Field confField = AbstractYarnClusterDescriptor.class.getDeclaredField("conf");
                    confField.setAccessible(true);
                    haYarnConf(yarnConf);
                    confField.set(clusterDescriptor, yarnConf);

                    YarnClusterClient clusterClient = clusterDescriptor.retrieve(applicationId);
                    clusterClient.setDetached(true);
                    return clusterClient;
            } catch(Exception e) {
                throw new RuntimeException(e);
            }
        }

        throw new UnsupportedOperationException("Haven't been developed yet!");
    }

    /**
     * 处理yarn HA的配置项
     */
    private static org.apache.hadoop.conf.Configuration haYarnConf(org.apache.hadoop.conf.Configuration yarnConf) {
        Iterator<Map.Entry<String, String>> iterator = yarnConf.iterator();
        while(iterator.hasNext()) {
            Map.Entry<String,String> entry = iterator.next();
            String key = entry.getKey();
            String value = entry.getValue();
            if(key.startsWith("yarn.resourcemanager.hostname.")) {
                String rm = key.substring("yarn.resourcemanager.hostname.".length());
                String addressKey = "yarn.resourcemanager.address." + rm;
                if(yarnConf.get(addressKey) == null) {
                    yarnConf.set(addressKey, value + ":" + YarnConfiguration.DEFAULT_RM_PORT);
                }
            }
        }
        return yarnConf;
    }
}
