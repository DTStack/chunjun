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

import com.dtstack.chunjun.options.ServerOptions;

import java.io.File;
import java.util.Properties;

/**
 * chunjun server 配置
 *
 * @author xuchao
 * @date 2023-06-13
 */
public class ChunJunConfig {

    private static final String DEFAULT_APP_NAME = "CHUNJUN-SESSION";

    private static final String DEFAULT_QUEUE = "default";

    /** 是否开启session check */
    private boolean enableSessionCheck = true;

    /** 是否开启自动部署session */
    private boolean enableSessionDeploy = true;

    /** 是否开启restful 服务，用于进行任务提交，日志查询，状态等查询 */
    private boolean enableRestful = true;

    private int serverPort = 18081;

    private String flinkConfDir;

    private String flinkLibDir;

    private String flinkPluginLibDir;

    private String chunJunLibDir;

    private String hadoopConfDir;

    private String applicationName = DEFAULT_APP_NAME;

    private String queue = DEFAULT_QUEUE;

    private String chunJunLogConfDir;

    /** 默认的日志级别 */
    private String logLevel = "INFO";

    public boolean isEnableSessionCheck() {
        return enableSessionCheck;
    }

    public static ChunJunConfig fromProperties(Properties config) {
        String flinkHome = config.getProperty(ServerOptions.FLINK_HOME_PATH_KEY);
        String chunjunHome = config.getProperty(ServerOptions.CHUNJUN_HOME_PATH_KEY);
        boolean sessionCheck =
                Boolean.parseBoolean(
                        (String)
                                config.getOrDefault(
                                        ServerOptions.CHUNJUN_SESSION_CHECK_KEY, "true"));
        String hadoopConfDir = config.getProperty(ServerOptions.HADOOP_CONF_PATH);
        boolean sessionDeploy =
                Boolean.valueOf(
                        (String)
                                config.getOrDefault(
                                        ServerOptions.CHUNJUN_SESSION_DEPLOY_KEY, "true"));
        boolean restful =
                Boolean.valueOf(
                        (String)
                                config.getOrDefault(
                                        ServerOptions.CHUNJUN_RESTFUL_ENABLE_KEY, "true"));
        Integer serverPort =
                Integer.valueOf(
                        (String)
                                config.getOrDefault(
                                        ServerOptions.CHUNJUN_SERVER_PORT_KEY, "18081"));
        String yarnQueue = (String) config.getOrDefault(ServerOptions.YARN_QUEUE_KEY, "default");
        String logLevel = (String) config.getOrDefault(ServerOptions.LOG_LEVEL_KEY, "INFO");

        if (flinkHome == null || flinkHome.isEmpty()) {
            throw new RuntimeException("flink home is null");
        }

        if (chunjunHome == null || chunjunHome.isEmpty()) {
            throw new RuntimeException("chunjun home is null");
        }

        if (hadoopConfDir == null || hadoopConfDir.isEmpty()) {
            // 通过环境变量获取
            hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
        }

        if (hadoopConfDir == null || hadoopConfDir.isEmpty()) {
            throw new RuntimeException("hadoop conf dir is null");
        }

        String flinkConfDir = flinkHome + File.separator + "conf";
        String flinkLibDir = flinkHome + File.separator + "lib";
        String flinkPluginsDir = flinkHome + File.separator + "plugins";

        String chunjunLibDir = chunjunHome + File.separator + "chunjun-dist";
        String chunjunLogConfDir =
                chunjunHome + File.separator + "conf" + File.separator + "logconf";

        ChunJunConfig chunJunConfig = new ChunJunConfig();
        chunJunConfig.setFlinkConfDir(flinkConfDir);
        chunJunConfig.setFlinkLibDir(flinkLibDir);
        chunJunConfig.setFlinkPluginLibDir(flinkPluginsDir);

        chunJunConfig.setChunJunLibDir(chunjunLibDir);
        chunJunConfig.setChunJunLogConfDir(chunjunLogConfDir);

        chunJunConfig.setHadoopConfDir(hadoopConfDir);

        chunJunConfig.setEnableSessionCheck(sessionCheck);
        chunJunConfig.setEnableSessionDeploy(sessionDeploy);
        chunJunConfig.setEnableRestful(restful);
        chunJunConfig.setServerPort(serverPort);

        chunJunConfig.setLogLevel(logLevel);
        chunJunConfig.setQueue(yarnQueue);

        return chunJunConfig;
    }

    public void setEnableSessionCheck(boolean enableSessionCheck) {
        this.enableSessionCheck = enableSessionCheck;
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

    public String getApplicationName() {
        return applicationName;
    }

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public boolean isEnableSessionDeploy() {
        return enableSessionDeploy;
    }

    public void setEnableSessionDeploy(boolean enableSessionDeploy) {
        this.enableSessionDeploy = enableSessionDeploy;
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

    public String getFlinkPluginLibDir() {
        return flinkPluginLibDir;
    }

    public void setFlinkPluginLibDir(String flinkPluginLibDir) {
        this.flinkPluginLibDir = flinkPluginLibDir;
    }

    public String getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

    public String getChunJunLogConfDir() {
        return chunJunLogConfDir;
    }

    public void setChunJunLogConfDir(String chunJunLogConfDir) {
        this.chunJunLogConfDir = chunJunLogConfDir;
    }

    public boolean isEnableRestful() {
        return enableRestful;
    }

    public void setEnableRestful(boolean enableRestful) {
        this.enableRestful = enableRestful;
    }

    public int getServerPort() {
        return serverPort;
    }

    public void setServerPort(int serverPort) {
        this.serverPort = serverPort;
    }
}
