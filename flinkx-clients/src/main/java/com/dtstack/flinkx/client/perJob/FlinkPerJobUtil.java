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
package com.dtstack.flinkx.client.perJob;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.PackagedProgram;

import com.dtstack.flinkx.util.ValueUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

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
            if (conProp.containsKey(JOBMANAGER_MEMORY_MB)) {
                jobManagerMemoryMb = Math.max(MIN_JM_MEMORY, ValueUtil.getInt(conProp.getProperty(JOBMANAGER_MEMORY_MB)));
            }
            if (conProp.containsKey(TASKMANAGER_MEMORY_MB)) {
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

    public static PackagedProgram buildProgram(ClusterSpecification clusterSpecification) throws Exception {
        String[] args = clusterSpecification.getProgramArgs();
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
