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

import com.dtstack.flinkx.util.ValueUtil;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;

import java.util.Properties;

/**
 * Date: 2019/09/11
 * Company: www.dtstack.com
 * @author tudou
 */
public class FlinkPerJobResourceUtil {
    /**
     * Minimum memory requirements, checked by the Client.
     * the minimum memory should be higher than the min heap cutoff
     */
    public final static int MIN_JM_MEMORY = 768;

    public final static String NUMBER_TASK_MANAGERS = "taskmanager.num";

    /**
     * the specification of this per-job mode cost
     * @param conProp taskParams
     * @return
     */
    public static ClusterSpecification createClusterSpecification(Properties conProp, Configuration flinkConfig){
        int jobManagerMemoryMb = ConfigurationUtils.getJobManagerHeapMemory(flinkConfig).getMebiBytes();
        int taskManagerMemoryMb = ConfigurationUtils.getTaskManagerHeapMemory(flinkConfig).getMebiBytes();
        int numberTaskManagers = flinkConfig.getInteger(NUMBER_TASK_MANAGERS, 1);
        int slots = flinkConfig.getInteger(TaskManagerOptions.NUM_TASK_SLOTS, 1);

        if(conProp != null){
            if(conProp.containsKey(JobManagerOptions.JOB_MANAGER_HEAP_MEMORY.key())){
                jobManagerMemoryMb = Math.max(MIN_JM_MEMORY, ValueUtil.getInt(conProp.getProperty(JobManagerOptions.JOB_MANAGER_HEAP_MEMORY.key())));
            }
            if(conProp.containsKey(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY.key())){
                taskManagerMemoryMb = Math.max(MIN_JM_MEMORY, ValueUtil.getInt(conProp.getProperty(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY.key())));
            }

            if (conProp.containsKey(NUMBER_TASK_MANAGERS)){
                numberTaskManagers = ValueUtil.getInt(conProp.get(NUMBER_TASK_MANAGERS));
            }

            if (conProp.containsKey(TaskManagerOptions.NUM_TASK_SLOTS.key())){
                slots = ValueUtil.getInt(conProp.get(TaskManagerOptions.NUM_TASK_SLOTS.key()));
            }
        }

        return new ClusterSpecification.ClusterSpecificationBuilder()
                .setMasterMemoryMB(jobManagerMemoryMb)
                .setTaskManagerMemoryMB(taskManagerMemoryMb)
                .setNumberTaskManagers(numberTaskManagers)
                .setSlotsPerTaskManager(slots)
                .createClusterSpecification();
    }
}