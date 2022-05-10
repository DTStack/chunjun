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
package com.dtstack.chunjun.options;

import com.dtstack.chunjun.constants.ConfigConstant;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.enums.ClusterMode;
import com.dtstack.chunjun.util.PropertiesUtil;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.commons.lang.StringUtils;

/**
 * Date: 2021/03/18 Company: www.dtstack.com
 *
 * @author tudou
 */
public class Options {
    @OptionRequired(description = "job type:sql or sync")
    private String jobType;

    @OptionRequired(description = "Running mode")
    private String mode = ClusterMode.local.name();

    @OptionRequired(description = "Job config")
    private String job;

    @OptionRequired(description = "Flink Job Name")
    private String jobName = "Flink_Job";

    @OptionRequired(description = "Flink configuration directory")
    private String flinkConfDir;

    @OptionRequired(description = "FlinkX dist dir")
    private String chunjunDistDir;

    @OptionRequired(description = "Yarn and Hadoop configuration directory")
    private String hadoopConfDir;

    @OptionRequired(description = "ext flinkLibJar")
    private String flinkLibDir;

    @OptionRequired(description = "env properties")
    private String confProp = "{}";

    @OptionRequired(description = "json modify")
    private String p = "";

    @OptionRequired(description = "plugin load mode, by classpath or shipfile")
    private String pluginLoadMode = "shipfile";

    @OptionRequired(description = "remote FlinkX dist dir")
    private String remoteChunjunDistDir;

    @OptionRequired(description = "sql ext jar,eg udf jar")
    private String addjar;

    @OptionRequired(description = "file add to ship file")
    private String addShipfile;

    private Configuration flinkConfiguration = null;

    public Configuration loadFlinkConfiguration() {
        if (flinkConfiguration == null) {
            Configuration dynamicConf = Configuration.fromMap(PropertiesUtil.confToMap(confProp));
            flinkConfiguration =
                    StringUtils.isEmpty(flinkConfDir)
                            ? new Configuration(dynamicConf)
                            : GlobalConfiguration.loadConfiguration(flinkConfDir, dynamicConf);
            if (StringUtils.isNotBlank(jobName)) {
                flinkConfiguration.setString(YarnConfigOptions.APPLICATION_NAME, jobName);
            }
            if (StringUtils.isNotBlank(hadoopConfDir)) {
                flinkConfiguration.setString(ConfigConstants.PATH_HADOOP_CONFIG, hadoopConfDir);
            }
            if (ConstantValue.CLASS_PATH_PLUGIN_LOAD_MODE.equalsIgnoreCase(pluginLoadMode)) {
                flinkConfiguration.setString(CoreOptions.CLASSLOADER_RESOLVE_ORDER, "child-first");
            } else {
                flinkConfiguration.setString(CoreOptions.CLASSLOADER_RESOLVE_ORDER, "parent-first");
            }
            flinkConfiguration.setString(ConfigConstant.FLINK_PLUGIN_LOAD_MODE_KEY, pluginLoadMode);

            flinkConfiguration.set(CoreOptions.CHECK_LEAKED_CLASSLOADER, false);
        }
        return flinkConfiguration;
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

    public String getFlinkConfDir() {
        return flinkConfDir;
    }

    public void setFlinkConfDir(String flinkConfDir) {
        this.flinkConfDir = flinkConfDir;
    }

    public String getChunjunDistDir() {
        return chunjunDistDir;
    }

    public void setChunjunDistDir(String chunjunDistDir) {
        this.chunjunDistDir = chunjunDistDir;
    }

    public String getHadoopConfDir() {
        return hadoopConfDir;
    }

    public void setHadoopConfDir(String hadoopConfDir) {
        this.hadoopConfDir = hadoopConfDir;
    }

    public String getFlinkLibDir() {
        return flinkLibDir;
    }

    public void setFlinkLibDir(String flinkLibDir) {
        this.flinkLibDir = flinkLibDir;
    }

    public String getConfProp() {
        return confProp;
    }

    public void setConfProp(String confProp) {
        this.confProp = confProp;
    }

    public String getP() {
        return p;
    }

    public void setP(String p) {
        this.p = p;
    }

    public String getPluginLoadMode() {
        return pluginLoadMode;
    }

    public void setPluginLoadMode(String pluginLoadMode) {
        this.pluginLoadMode = pluginLoadMode;
    }

    public String getRemoteChunjunDistDir() {
        return remoteChunjunDistDir;
    }

    public void setRemoteChunjunDistDir(String remoteChunjunDistDir) {
        this.remoteChunjunDistDir = remoteChunjunDistDir;
    }

    public String getAddjar() {
        return addjar;
    }

    public void setAddjar(String addjar) {
        this.addjar = addjar;
    }

    public String getAddShipfile() {
        return addShipfile;
    }

    public void setAddShipfile(String addShipfile) {
        this.addShipfile = addShipfile;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getJobType() {
        return jobType;
    }

    public void setJobType(String jobType) {
        this.jobType = jobType;
    }

    @Override
    public String toString() {
        return "Options{"
                + "jobType='"
                + jobType
                + '\''
                + ", mode='"
                + mode
                + '\''
                + ", job='"
                + job
                + '\''
                + ", jobName='"
                + jobName
                + '\''
                + ", flinkConfDir='"
                + flinkConfDir
                + '\''
                + ", flinkxDistDir='"
                + chunjunDistDir
                + '\''
                + ", hadoopConfDir='"
                + hadoopConfDir
                + '\''
                + ", flinkLibDir='"
                + flinkLibDir
                + '\''
                + ", confProp='"
                + confProp
                + '\''
                + ", p='"
                + p
                + '\''
                + ", pluginLoadMode='"
                + pluginLoadMode
                + '\''
                + ", remotePluginPath='"
                + remoteChunjunDistDir
                + '\''
                + ", addjar='"
                + addjar
                + '\''
                + ", addShipfile='"
                + addShipfile
                + '\''
                + ", flinkConfiguration="
                + flinkConfiguration
                + '}';
    }
}
