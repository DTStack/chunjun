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

package org.apache.flink.client.deployment;

import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.net.URL;
import java.util.List;

/**
 * Description of the cluster to start by the {@link ClusterDescriptor}.
 */
public final class ClusterSpecification {
    private final int masterMemoryMB;
    private final int taskManagerMemoryMB;
    private final int numberTaskManagers;
    private final int slotsPerTaskManager;
    private final int priority;

    private int parallelism;
    private Configuration configuration;
    private YarnConfiguration yarnConfiguration;
    private JobGraph jobGraph;
    private SavepointRestoreSettings spSetting = SavepointRestoreSettings.none();
    private List<URL> classpaths;
    private String entryPointClass;
    private String[] programArgs;
    private File jarFile;
    private boolean createProgramDelay = false;
    private PackagedProgram program;

    private ClusterSpecification(int masterMemoryMB, int taskManagerMemoryMB, int numberTaskManagers, int slotsPerTaskManager, int parallelism, int priority) {
        this.masterMemoryMB = masterMemoryMB;
        this.taskManagerMemoryMB = taskManagerMemoryMB;
        this.numberTaskManagers = numberTaskManagers;
        this.slotsPerTaskManager = slotsPerTaskManager;
        this.parallelism = parallelism;
        this.priority = priority;
    }

    public PackagedProgram getProgram() {
        return program;
    }

    public void setProgram(PackagedProgram program) {
        this.program = program;
    }

    public YarnConfiguration getYarnConfiguration() {
        return yarnConfiguration;
    }

    public void setYarnConfiguration(YarnConfiguration yarnConfiguration) {
        this.yarnConfiguration = yarnConfiguration;
    }

    public JobGraph getJobGraph() {
        return jobGraph;
    }

    public void setJobGraph(JobGraph jobGraph) {
        this.jobGraph = jobGraph;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public int getMasterMemoryMB() {
        return masterMemoryMB;
    }

    public int getTaskManagerMemoryMB() {
        return taskManagerMemoryMB;
    }

    public int getNumberTaskManagers() {
        return numberTaskManagers;
    }

    public int getSlotsPerTaskManager() {
        return slotsPerTaskManager;
    }

    public int getPriority(){
        return priority;
    }

    public SavepointRestoreSettings getSpSetting() {
        return spSetting;
    }

    public void setSpSetting(SavepointRestoreSettings spSetting) {
        this.spSetting = spSetting;
    }

    public List<URL> getClasspaths() {
        return classpaths;
    }

    public void setClasspaths(List<URL> classpaths) {
        this.classpaths = classpaths;
    }

    public String getEntryPointClass() {
        return entryPointClass;
    }

    public void setEntryPointClass(String entryPointClass) {
        this.entryPointClass = entryPointClass;
    }

    public String[] getProgramArgs() {
        return programArgs;
    }

    public void setProgramArgs(String[] programArgs) {
        this.programArgs = programArgs;
    }

    public File getJarFile() {
        return jarFile;
    }

    public void setJarFile(File jarFile) {
        this.jarFile = jarFile;
    }

    public boolean isCreateProgramDelay() {
        return createProgramDelay;
    }

    public void setCreateProgramDelay(boolean createProgramDelay) {
        this.createProgramDelay = createProgramDelay;
    }

    @Override
    public String toString() {
        return "ClusterSpecification{" +
                "masterMemoryMB=" + masterMemoryMB +
                ", taskManagerMemoryMB=" + taskManagerMemoryMB +
                ", numberTaskManagers=" + numberTaskManagers +
                ", slotsPerTaskManager=" + slotsPerTaskManager +
                ", priority=" + priority +
                '}';
    }

    /**
     * Builder for the {@link ClusterSpecification} instance.
     */
    public static class ClusterSpecificationBuilder {
        private int masterMemoryMB = 768;
        private int taskManagerMemoryMB = 1024;
        private int numberTaskManagers = 1;
        private int slotsPerTaskManager = 1;
        private int parallelism = 1;
        private int priority = 0;

        public ClusterSpecificationBuilder setMasterMemoryMB(int masterMemoryMB) {
            this.masterMemoryMB = masterMemoryMB;
            return this;
        }

        public ClusterSpecificationBuilder setTaskManagerMemoryMB(int taskManagerMemoryMB) {
            this.taskManagerMemoryMB = taskManagerMemoryMB;
            return this;
        }

        public ClusterSpecificationBuilder setNumberTaskManagers(int numberTaskManagers) {
            this.numberTaskManagers = numberTaskManagers;
            return this;
        }

        public ClusterSpecificationBuilder setSlotsPerTaskManager(int slotsPerTaskManager) {
            this.slotsPerTaskManager = slotsPerTaskManager;
            return this;
        }

        public ClusterSpecificationBuilder setPriority(int priority){
            this.priority = priority;
            return this;
        }

        public ClusterSpecificationBuilder setParallelism(int parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        public ClusterSpecification createClusterSpecification() {
            return new ClusterSpecification(
                    masterMemoryMB,
                    taskManagerMemoryMB,
                    numberTaskManagers,
                    slotsPerTaskManager,
                    parallelism,
                    priority);
        }
    }
}
