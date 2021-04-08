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

import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.ValueUtil;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Properties;

import static com.dtstack.flinkx.constants.ConfigConstant.YARN_RESOURCE_MANAGER_WEBAPP_ADDRESS_KEY;

/**
 * Date: 2019/09/11
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class FlinkPerJobUtil {
    /**
     * Minimum memory requirements, checked by the Client.
     * the minimum memory should be higher than the min heap cutoff
     */
    public final static int MIN_JM_MEMORY = 768;
    public final static int MIN_TM_MEMORY = 1024;
    public final static String JOBMANAGER_MEMORY_MB = "jobmanager.memory.process.size";
    public final static String TASKMANAGER_MEMORY_MB = "taskmanager.memory.process.size";
    public final static String SLOTS_PER_TASKMANAGER = "taskmanager.slots";
    private static final Logger LOG = LoggerFactory.getLogger(FlinkPerJobUtil.class);

    /**
     * the specification of this per-job mode cost
     *
     * @param conProp taskParams
     * @return
     */
    public static ClusterSpecification createClusterSpecification(Properties conProp) {
        int jobManagerMemoryMb = 768;
        int taskManagerMemoryMb = 1024;
        int slotsPerTaskManager = 1;

        if (conProp != null) {
            if (conProp.contains(JOBMANAGER_MEMORY_MB)) {
                jobManagerMemoryMb = Math.max(MIN_JM_MEMORY, ValueUtil.getInt(conProp.getProperty(JOBMANAGER_MEMORY_MB)));
            }
            if (conProp.contains(TASKMANAGER_MEMORY_MB)) {
                taskManagerMemoryMb = Math.max(MIN_JM_MEMORY, ValueUtil.getInt(conProp.getProperty(TASKMANAGER_MEMORY_MB)));
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

    public static String getUrlFormat(YarnConfiguration yarnConf, YarnClient yarnClient) {
        String url = "";
        try {
            Field rmClientField = yarnClient.getClass().getDeclaredField("rmClient");
            rmClientField.setAccessible(true);
            Object rmClient = rmClientField.get(yarnClient);

            Field hField = rmClient.getClass().getSuperclass().getDeclaredField("h");
            hField.setAccessible(true);
            //获取指定对象中此字段的值
            Object h = hField.get(rmClient);
            Object currentProxy = null;

            try {
                Field currentProxyField = h.getClass().getDeclaredField("currentProxy");
                currentProxyField.setAccessible(true);
                currentProxy = currentProxyField.get(h);
            } catch (Exception e) {
                //兼容Hadoop 2.7.3.2.6.4.91-3
                LOG.warn("get currentProxy error:{}", ExceptionUtil.getErrorMessage(e));
                Field proxyDescriptorField = h.getClass().getDeclaredField("proxyDescriptor");
                proxyDescriptorField.setAccessible(true);
                Object proxyDescriptor = proxyDescriptorField.get(h);
                Field currentProxyField = proxyDescriptor.getClass().getDeclaredField("proxyInfo");
                currentProxyField.setAccessible(true);
                currentProxy = currentProxyField.get(proxyDescriptor);
            }

            Field proxyInfoField = currentProxy.getClass().getDeclaredField("proxyInfo");
            proxyInfoField.setAccessible(true);
            String proxyInfoKey = (String) proxyInfoField.get(currentProxy);

            String key = YARN_RESOURCE_MANAGER_WEBAPP_ADDRESS_KEY + "." + proxyInfoKey;
            String addr = yarnConf.get(key);

            if (addr == null) {
                addr = yarnConf.get(YARN_RESOURCE_MANAGER_WEBAPP_ADDRESS_KEY);
            }

            return String.format("http://%s/proxy", addr);
        } catch (Exception e) {
            LOG.warn("get monitor error:{}", ExceptionUtil.getErrorMessage(e));
        }

        return url;
    }

    public static PackagedProgram buildProgram(String monitorUrl, ClusterSpecification clusterSpecification) throws Exception {
        String[] args = clusterSpecification.getProgramArgs();
        for (int i = 0; i < args.length; i++) {
            if ("-monitor".equals(args[i])) {
                args[i + 1] = monitorUrl;
                break;
            }
        }

        return PackagedProgram.newBuilder()
                .setJarFile(clusterSpecification.getJarFile())
                .setUserClassPaths(clusterSpecification.getClasspaths())
                .setEntryPointClassName(clusterSpecification.getEntryPointClass())
                .setConfiguration(clusterSpecification.getConfiguration())
                .setSavepointRestoreSettings(clusterSpecification.getSpSetting())
                .setArguments(args)
                .build();
    }
}