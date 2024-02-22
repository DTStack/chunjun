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
package com.dtstack.chunjun.server;

import com.dtstack.chunjun.config.ChunJunConfig;
import com.dtstack.chunjun.config.ChunJunServerOptions;
import com.dtstack.chunjun.config.SessionConfig;
import com.dtstack.chunjun.config.WebConfig;
import com.dtstack.chunjun.config.YarnAppConfig;
import com.dtstack.chunjun.restapi.WebServer;
import com.dtstack.chunjun.server.util.EnvUtil;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.configuration.YarnConfigOptionsInternal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_LIB_DIR;
import static org.apache.flink.yarn.configuration.YarnConfigOptions.FLINK_DIST_JAR;

/**
 * 启动 chunjun server 服务
 *
 * @author xuchao
 * @date 2023-06-13
 */
public class ServerLauncher {

    private static final Logger LOG = LoggerFactory.getLogger(ServerLauncher.class);

    private ChunJunConfig chunJunConfig;

    private int port = 18081;

    private SessionManager sessionManager;

    private WebServer webServer;

    private SessionConfig sessionConfig;

    public void startServer() throws Exception {
        sessionConfig =
                new SessionConfig(
                        chunJunConfig.getFlinkConfDir(),
                        chunJunConfig.getHadoopConfDir(),
                        chunJunConfig.getFlinkLibDir(),
                        chunJunConfig.getChunJunLibDir());

        YarnAppConfig yarnAppConfig = new YarnAppConfig();
        yarnAppConfig.setApplicationName(chunJunConfig.getApplicationName());
        yarnAppConfig.setQueue(chunJunConfig.getQueue());

        sessionConfig.setAppConfig(yarnAppConfig);
        sessionConfig.loadFlinkConfiguration();
        sessionConfig.loadHadoopConfiguration();

        // 初始化环境变量,设置env相关的属性配置
        initEnv();
        // 启动session 检查
        sessionManagerStart();
        // 启动restapi 服务
        webStart();

        // 注册shutdown hook
        Runtime.getRuntime().addShutdownHook(new ShutdownThread(this));
    }

    public void stop() {
        sessionManager.stopSessionCheck();
        sessionManager.stopSessionDeploy();
    }

    public ChunJunConfig getChunJunConfig() {
        return chunJunConfig;
    }

    public void setChunJunConfig(ChunJunConfig chunJunConfig) {
        this.chunJunConfig = chunJunConfig;
    }

    public void initEnv() throws Exception {
        // 如果环境变量没有设置ENV_FLINK_LIB_DIR, 但是进程要求必须传对应的参数，所以可以通过进程来设置
        // upload and register ship-only files，Plugin files only need to be shipped and should not
        // be added to classpath.
        Configuration configuration = sessionConfig.getFlinkConfig();

        if (System.getenv().get(ENV_FLINK_LIB_DIR) == null) {
            LOG.warn(
                    "Environment variable '{}' not set and ship files have not been provided manually.",
                    ENV_FLINK_LIB_DIR);
            Map<String, String> envMap = new HashMap<>();
            envMap.put(ENV_FLINK_LIB_DIR, sessionConfig.getFlinkLibDir());
            EnvUtil.setEnv(envMap);
        }

        // 添加ChunJun dist 目录到configuration
        String chunJunLibDir = sessionConfig.getChunJunLibDir();
        if (chunJunLibDir != null) {
            chunJunLibDir =
                    chunJunLibDir.startsWith("file:") ? chunJunLibDir : "file:" + chunJunLibDir;
            configuration.set(ChunJunServerOptions.CHUNJUN_DIST_PATH, chunJunLibDir);
        }

        // 添加对flinkplugin 的环境变量的设置
        String envFlinkPluginDir = System.getenv().get(ConfigConstants.ENV_FLINK_PLUGINS_DIR);
        if (envFlinkPluginDir == null) {
            Map<String, String> envMap = new HashMap<>();
            envMap.put(ConfigConstants.ENV_FLINK_PLUGINS_DIR, chunJunConfig.getFlinkPluginLibDir());
            EnvUtil.setEnv(envMap);
        }

        // 添加日志路径环境变量
        String logConfigPath = chunJunConfig.getChunJunLogConfDir();
        LOG.info("chunjun session log level:{}", chunJunConfig.getLogLevel());
        String levelPath =
                logConfigPath + File.separatorChar + chunJunConfig.getLogLevel().toLowerCase();

        Preconditions.checkArgument(
                new File(logConfigPath).exists(),
                levelPath + " is not exists. please check log conf dir path.");

        sessionConfig
                .getFlinkConfig()
                .set(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE, levelPath);

        if (configuration.get(FLINK_DIST_JAR) == null) {
            // 设置yarnClusterDescriptor yarn.flink-dist-jar
            File flinkLibDir = new File(sessionConfig.getFlinkLibDir());
            if (!flinkLibDir.isDirectory()) {
                throw new RuntimeException(
                        String.format(
                                "param set flinkLibDir(%s) is not a dir, please check it.",
                                sessionConfig.getFlinkLibDir()));
            }
            Arrays.stream(flinkLibDir.listFiles())
                    .map(
                            file -> {
                                try {
                                    if (file.toURI().toURL().toString().contains("flink-dist")) {
                                        configuration.setString(
                                                FLINK_DIST_JAR, file.toURI().toURL().toString());
                                    }
                                } catch (MalformedURLException e) {
                                    LOG.warn("", e);
                                }

                                return file;
                            })
                    .collect(Collectors.toList());
        }
    }

    public void sessionManagerStart() {
        sessionManager = new SessionManager(sessionConfig);
        // 是否启动session check 服务
        if (chunJunConfig.isEnableSessionCheck()) {
            LOG.info("open session check!");
            sessionManager.startSessionCheck();
        }

        // 是否开启session deploy 服务
        if (chunJunConfig.isEnableSessionDeploy()) {
            LOG.info("open session deploy!");
            sessionManager.startSessionDeploy();
        }
    }

    public void webStart() {
        if (chunJunConfig.isEnableRestful()) {
            LOG.info("start web server ");
            WebConfig webConfig = new WebConfig();
            webConfig.setPort(port);
            webServer = new WebServer(webConfig, sessionManager);
            webServer.startServer();
        }
    }

    private static class ShutdownThread extends Thread {
        private ServerLauncher serverLauncher;

        public ShutdownThread(ServerLauncher serverLauncher) {
            this.serverLauncher = serverLauncher;
        }

        public void run() {
            LOG.info("shutting down the chunjun serverLancher.");
            try {
                serverLauncher.stop();
            } catch (Exception e) {
                LOG.error("failed to shut down the chunjun server lancher,", e);
            }

            LOG.info("chunjun serverLancher has been shutdown.");
        }
    }

    public static void main(String[] args) throws Exception {
        ServerLauncher serverLauncher = new ServerLauncher();
        Properties argProp = new Properties();
        String appConfPath = "./conf/application.properties";
        File configFile = new File(appConfPath);
        if (!configFile.exists()) {
            throw new RuntimeException(String.format("%s config file is not exists.", appConfPath));
        }

        argProp.load(new FileInputStream(configFile));
        ChunJunConfig chunJunConfig = ChunJunConfig.fromProperties(argProp);
        serverLauncher.setChunJunConfig(chunJunConfig);

        serverLauncher.startServer();
        LOG.info("start chunjun server success!");
    }
}
