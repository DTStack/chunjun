/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.options;

import com.dtstack.flinkx.enums.ClusterMode;

/**
 * This class define commandline options for the Launcher program
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class Options {

    @OptionRequired(description = "Running mode")
    private String mode = ClusterMode.local.name();

    @OptionRequired(required = true, description = "Job config")
    private String job;

    @OptionRequired(description = "Monitor Addresses")
    private String monitor;

    @OptionRequired(description = "Job unique id")
    private String jobid = "Flink Job";

    @OptionRequired(description = "Flink configuration directory")
    private String flinkconf;

    @OptionRequired(description = "env properties")
    private String pluginRoot;

    @OptionRequired(description = "Yarn and Hadoop configuration directory")
    private String yarnconf;

    @OptionRequired(description = "Task parallelism")
    private String parallelism = "1";

    @OptionRequired(description = "Task priority")
    private String priority = "1";

    @OptionRequired(description = "Yarn queue")
    private String queue = "default";

    @OptionRequired(description = "ext flinkLibJar")
    private String flinkLibJar;

    @OptionRequired(description = "env properties")
    private String confProp = "{}";

    /**
     * savepoint
     */
    @OptionRequired(description = "savepoint path")
    private String s;

    @OptionRequired(description = "plugin load mode, by classpath or shipfile")
    private String pluginLoadMode = "shipfile";

    @OptionRequired(description = "applicationId on yarn cluster")
    private String appId;

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getS() {
        return s;
    }

    public void setS(String s) {
        this.s = s;
    }

    public String getConfProp() {
        return confProp;
    }

    public void setConfProp(String confProp) {
        this.confProp = confProp;
    }

    public String getParallelism() {
        return parallelism;
    }

    public void setParallelism(String parallelism) {
        this.parallelism = parallelism;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }

    public String getMonitor() {
        return monitor;
    }

    public void setMonitor(String monitor) {
        this.monitor = monitor;
    }

    public String getJobid() {
        return jobid;
    }

    public void setJobid(String jobid) {
        this.jobid = jobid;
    }

    public String getFlinkconf() {
        return flinkconf;
    }

    public void setFlinkconf(String flinkconf) {
        this.flinkconf = flinkconf;
    }

    public String getPluginRoot() {
        return pluginRoot;
    }

    public void setPluginRoot(String pluginRoot) {
        this.pluginRoot = pluginRoot;
    }

    public String getYarnconf() {
        return yarnconf;
    }

    public void setYarnconf(String yarnconf) {
        this.yarnconf = yarnconf;
    }

    public String getPriority() {
        return priority;
    }

    public void setPriority(String priority) {
        this.priority = priority;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getFlinkLibJar() {
        return flinkLibJar;
    }

    public void setFlinkLibJar(String flinkLibJar) {
        this.flinkLibJar = flinkLibJar;
    }

    public String getPluginLoadMode() {
        return pluginLoadMode;
    }

    public void setPluginLoadMode(String pluginLoadMode) {
        this.pluginLoadMode = pluginLoadMode;
    }
}
