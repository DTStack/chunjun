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

import com.dtstack.flinkx.constants.ConfigConstant;
import com.dtstack.flinkx.enums.ClusterMode;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import static com.dtstack.flinkx.constants.ConstantValue.CLASS_PATH_PLUGIN_LOAD_MODE;

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

    @OptionRequired(description = "json modify")
    private String p = "";

    @OptionRequired(description = "savepoint path")
    private String s;

    @OptionRequired(description = "plugin load mode, by classpath or shipfile")
    private String pluginLoadMode = "shipfile";

    @OptionRequired(description = "kerberos krb5conf")
    private String krb5conf ;

    @OptionRequired(description = "kerberos keytabPath")
    private String keytab ;

    @OptionRequired(description = "kerberos principal")
    private String principal ;

    @OptionRequired(description = "applicationId on yarn cluster")
    private String appId;

    @OptionRequired(description = "Sync remote plugin root path")
    private String remotePluginPath;

    private Configuration flinkConfiguration = null;

    public Configuration loadFlinkConfiguration() {
        if(flinkConfiguration == null){
            flinkConfiguration = StringUtils.isEmpty(flinkconf) ? new Configuration() : GlobalConfiguration.loadConfiguration(flinkconf);
            if (StringUtils.isNotBlank(queue)) {
                flinkConfiguration.setString(YarnConfigOptions.APPLICATION_QUEUE, queue);
            }
            if (StringUtils.isNotBlank(jobid)) {
                flinkConfiguration.setString(YarnConfigOptions.APPLICATION_NAME, jobid);
            }
            if(StringUtils.isNotBlank(yarnconf)){
                flinkConfiguration.setString(ConfigConstants.PATH_HADOOP_CONFIG, yarnconf);
            }
            if(CLASS_PATH_PLUGIN_LOAD_MODE.equalsIgnoreCase(pluginLoadMode)){
                flinkConfiguration.setString(CoreOptions.CLASSLOADER_RESOLVE_ORDER, "child-first");
            }else{
                flinkConfiguration.setString(CoreOptions.CLASSLOADER_RESOLVE_ORDER, "parent-first");
            }
            flinkConfiguration.setString(ConfigConstant.FLINK_PLUGIN_LOAD_MODE_KEY, pluginLoadMode);
        }
        return flinkConfiguration;
    }

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

    public String getRemotePluginPath() {
        return remotePluginPath;
    }

    public void setRemotePluginPath(String remotePluginPath) {
        this.remotePluginPath = remotePluginPath;
    }

    public String getP() {
        return p;
    }

    public void setP(String p) {
        this.p = p;
    }

    public String getKrb5conf() {
        return krb5conf;
    }

    public void setKrb5conf(String krb5conf) {
        this.krb5conf = krb5conf;
    }

    public String getKeytab() {
        return keytab;
    }

    public void setKeytab(String keytab) {
        this.keytab = keytab;
    }

    public String getPrincipal() {
        return principal;
    }

    public void setPrincipal(String principal) {
        this.principal = principal;
    }

    @Override
    public String toString() {
        return "Options{" +
                "mode='" + mode + '\'' +
                ", job='" + job + '\'' +
                ", monitor='" + monitor + '\'' +
                ", jobid='" + jobid + '\'' +
                ", flinkconf='" + flinkconf + '\'' +
                ", pluginRoot='" + pluginRoot + '\'' +
                ", yarnconf='" + yarnconf + '\'' +
                ", parallelism='" + parallelism + '\'' +
                ", priority='" + priority + '\'' +
                ", queue='" + queue + '\'' +
                ", flinkLibJar='" + flinkLibJar + '\'' +
                ", confProp='" + confProp + '\'' +
                ", p='" + p + '\'' +
                ", s='" + s + '\'' +
                ", pluginLoadMode='" + pluginLoadMode + '\'' +
                ", appId='" + appId + '\'' +
                ", remotePluginPath='" + remotePluginPath + '\'' +
                ", krb5conf='" + krb5conf + '\'' +
                ", keytab='" + keytab + '\'' +
                ", principal='" + principal + '\'' +
                '}';
    }
}
