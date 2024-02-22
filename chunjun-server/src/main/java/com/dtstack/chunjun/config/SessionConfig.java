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
package com.dtstack.chunjun.config;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.commons.lang3.StringUtils;

/**
 * session 指定的相关配置
 *
 * @author xuchao
 * @date 2023-05-22
 */
public class SessionConfig {

    private String flinkLibDir;

    private String chunJunLibDir;

    private String flinkConfDir;

    private String hadoopConfDir;

    protected YarnAppConfig appConfig;

    protected Configuration flinkConfig;

    protected HadoopConfig hadoopConfig;

    public SessionConfig(
            String flinkConfDir, String hadoopConfDir, String flinkLibDir, String chunJunLibDir) {
        this.flinkConfDir = flinkConfDir;
        this.hadoopConfDir = hadoopConfDir;
        this.flinkLibDir = flinkLibDir;
        this.chunJunLibDir = chunJunLibDir;
    }

    public YarnAppConfig getAppConfig() {
        return appConfig;
    }

    public void setAppConfig(YarnAppConfig appConfig) {
        this.appConfig = appConfig;
    }

    public Configuration getFlinkConfig() {
        return flinkConfig;
    }

    public void setFlinkConfig(Configuration flinkConfig) {
        this.flinkConfig = flinkConfig;
    }

    public HadoopConfig getHadoopConfig() {
        return hadoopConfig;
    }

    public void setHadoopConfig(HadoopConfig hadoopConfig) {
        this.hadoopConfig = hadoopConfig;
    }

    public String getFlinkLibDir() {
        return flinkLibDir;
    }

    public void setFlinkLibDir(String flinkLibDir) {
        this.flinkLibDir = flinkLibDir;
    }

    public String getChunJunLibDir() {
        return chunJunLibDir;
    }

    public void setChunJunLibDir(String chunJunLibDir) {
        this.chunJunLibDir = chunJunLibDir;
    }

    public String getFlinkConfDir() {
        return flinkConfDir;
    }

    public void setFlinkConfDir(String flinkConfDir) {
        this.flinkConfDir = flinkConfDir;
    }

    public String getHadoopConfDir() {
        return hadoopConfDir;
    }

    public void setHadoopConfDir(String hadoopConfDir) {
        this.hadoopConfDir = hadoopConfDir;
    }

    public void loadFlinkConfiguration() {
        if (flinkConfig == null) {
            flinkConfig =
                    StringUtils.isEmpty(flinkConfDir)
                            ? new Configuration()
                            : GlobalConfiguration.loadConfiguration(flinkConfDir);
            if (StringUtils.isNotBlank(appConfig.getApplicationName())) {
                flinkConfig.setString(
                        YarnConfigOptions.APPLICATION_NAME, appConfig.getApplicationName());
            }

            if (StringUtils.isNotBlank(hadoopConfDir)) {
                flinkConfig.setString(ConfigConstants.PATH_HADOOP_CONFIG, hadoopConfDir);
            }

            //            if
            // (ConstantValue.CLASS_PATH_PLUGIN_LOAD_MODE.equalsIgnoreCase(pluginLoadMode)) {
            //                flinkConfig.setString(CoreOptions.CLASSLOADER_RESOLVE_ORDER,
            // "child-first");
            //            } else {
            //                flinkConfig.setString(CoreOptions.CLASSLOADER_RESOLVE_ORDER,
            // "parent-first");
            //            }
            //            flinkConfig.setString(ConfigConstant.FLINK_PLUGIN_LOAD_MODE_KEY,
            // pluginLoadMode);
        }
    }

    public void loadHadoopConfiguration() {
        HadoopConfig hadoopConfig = new HadoopConfig(hadoopConfDir);
        hadoopConfig.initHadoopConf(null);
        hadoopConfig.initYarnConf(null);
        this.hadoopConfig = hadoopConfig;
    }
}
