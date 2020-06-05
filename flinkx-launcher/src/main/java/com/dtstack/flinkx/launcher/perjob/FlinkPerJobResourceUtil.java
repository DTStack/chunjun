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
    public final static int MIN_TM_MEMORY = 768;

    public final static String JOBMANAGER_MEMORY_MB = "jobmanager.memory.mb";
    public final static String TASKMANAGER_MEMORY_MB = "taskmanager.memory.mb";
    public final static String NUMBER_TASK_MANAGERS = "taskmanager.num";
    public final static String SLOTS_PER_TASKMANAGER = "taskmanager.slots";

    /**
     * the specification of this per-job mode cost
     * @param conProp taskParams
     * @return
     */
    public static ClusterSpecification createClusterSpecification(Properties conProp){
        int jobmanagerMemoryMb = 768;
        int taskmanagerMemoryMb = 768;
        int numberTaskManagers = 1;
        int slotsPerTaskManager = 1;

        if(conProp != null){
            if(conProp.contains(JOBMANAGER_MEMORY_MB)){
                jobmanagerMemoryMb = Math.max(MIN_JM_MEMORY, ValueUtil.getInt(conProp.getProperty(JOBMANAGER_MEMORY_MB)));
            }
            if(conProp.contains(TASKMANAGER_MEMORY_MB)){
                taskmanagerMemoryMb = Math.max(MIN_JM_MEMORY, ValueUtil.getInt(conProp.getProperty(TASKMANAGER_MEMORY_MB)));
            }

            if (conProp.containsKey(NUMBER_TASK_MANAGERS)){
                numberTaskManagers = ValueUtil.getInt(conProp.get(NUMBER_TASK_MANAGERS));
            }

            if (conProp.containsKey(SLOTS_PER_TASKMANAGER)){
                slotsPerTaskManager = ValueUtil.getInt(conProp.get(SLOTS_PER_TASKMANAGER));
            }
        }

        return new ClusterSpecification.ClusterSpecificationBuilder()
                .setMasterMemoryMB(jobmanagerMemoryMb)
                .setTaskManagerMemoryMB(taskmanagerMemoryMb)
                .setNumberTaskManagers(numberTaskManagers)
                .setSlotsPerTaskManager(slotsPerTaskManager)
                .createClusterSpecification();
    }
}