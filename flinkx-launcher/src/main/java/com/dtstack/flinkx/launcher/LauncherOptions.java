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

/**
 * This class define commandline options for the Launcher program
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class LauncherOptions {

      private String mode;

      private String job;

      private String monitor;

      private String jobid;

      private String flinkconf;

      private String plugin;

      private String yarnconf;

      private int parallelism = 1;

    private int priority = 1;

      private String queue;

      private String flinkLibJar;

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
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

    public String getPlugin() {
        return plugin;
    }

    public void setPlugin(String plugin) {
        this.plugin = plugin;
    }

    public String getYarnconf() {
        return yarnconf;
    }

    public void setYarnconf(String yarnconf) {
        this.yarnconf = yarnconf;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
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
}
