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
package com.dtstack.chunjun.client;

import com.dtstack.chunjun.annotation.NotNull;
import com.dtstack.chunjun.classloader.ClassLoaderManager;
import com.dtstack.chunjun.client.exception.DeploymentException;
import com.dtstack.chunjun.client.kubernetes.KubernetesApplicationClusterClientHelper;
import com.dtstack.chunjun.client.kubernetes.KubernetesSessionClusterClientHelper;
import com.dtstack.chunjun.client.local.LocalClusterClientHelper;
import com.dtstack.chunjun.client.standalone.StandaloneClusterClientHelper;
import com.dtstack.chunjun.client.yarn.YarnPerJobClusterClientHelper;
import com.dtstack.chunjun.client.yarn.YarnSessionClusterClientHelper;
import com.dtstack.chunjun.enums.ClusterMode;
import com.dtstack.chunjun.options.OptionParser;
import com.dtstack.chunjun.options.Options;
import com.dtstack.chunjun.util.ExecuteProcessHelper;

import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.ConfigConstants;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Map;

/** Chunjun commandline Launcher */
@Slf4j
public class Launcher {
    public static final String KEY_CHUNJUN_HOME = "CHUNJUN_HOME";
    public static final String KEY_FLINK_HOME = "FLINK_HOME";
    public static final String KEY_HADOOP_HOME = "HADOOP_HOME";

    public static final String PLUGINS_DIR_NAME = "chunjun-dist";

    public static void main(String[] args) throws Exception {
        // 1. parse command into options.
        OptionParser optionParser = new OptionParser(args);
        Options launcherOptions = optionParser.getOptions();

        findDefaultConfigDir(launcherOptions);

        List<String> argList = optionParser.getProgramExeArgList();

        // 将argList转化为HashMap，方便通过参数名称来获取参数值
        Map<String, String> commandMap = Maps.newHashMap();
        for (int i = 0; i < argList.size(); i += 2) {
            commandMap.put(argList.get(i), argList.get(i + 1));
        }

        // 清空list，填充修改后的参数值
        argList.clear();
        for (int i = 0; i < commandMap.size(); i++) {
            argList.add(commandMap.keySet().toArray()[i].toString());
            argList.add(commandMap.values().toArray()[i].toString());
        }

        JobDeployer jobDeployer = new JobDeployer(launcherOptions, argList);

        ClusterClientHelper<?> clusterClientHelper = createHelper(launcherOptions.getMode());

        // add ext class
        URLClassLoader urlClassLoader = (URLClassLoader) Launcher.class.getClassLoader();
        List<URL> jarUrlList = ExecuteProcessHelper.getExternalJarUrls(launcherOptions.getAddjar());
        ClassLoaderManager.loadExtraJar(jarUrlList, urlClassLoader);
        try (ClusterClient<?> client = clusterClientHelper.submit(jobDeployer)) {
            if (null != client) {
                log.info(client.getClusterId() + " submit successfully.");
            }
        }
    }

    private static ClusterClientHelper<?> createHelper(String mode) {
        switch (ClusterMode.getByName(mode)) {
            case local:
                return new LocalClusterClientHelper();
            case standalone:
                return new StandaloneClusterClientHelper();
            case yarnSession:
                return new YarnSessionClusterClientHelper();
            case yarnPerJob:
                return new YarnPerJobClusterClientHelper();
            case yarnApplication:
                throw new DeploymentException(
                        "Application Mode not supported by Yarn deployments.");
            case kubernetesSession:
                return new KubernetesSessionClusterClientHelper();
            case kubernetesPerJob:
                throw new DeploymentException(
                        "Per-Job Mode not supported by Kubernetes deployments.");
            case kubernetesApplication:
                return new KubernetesApplicationClusterClientHelper();
            default:
                throw new DeploymentException(mode + " Mode not supported.");
        }
    }

    private static void findDefaultConfigDir(Options launcherOptions)
            throws ClusterDeploymentException {
        findDefaultChunJunDistDir(launcherOptions);

        if (ClusterMode.local.name().equalsIgnoreCase(launcherOptions.getMode())) {
            return;
        }

        findDefaultFlinkConf(launcherOptions);
        findDefaultHadoopConf(launcherOptions);
    }

    private static void findDefaultHadoopConf(Options launcherOptions) {
        if (StringUtils.isNotEmpty(launcherOptions.getHadoopConfDir())) {
            return;
        }

        String hadoopHome = getSystemProperty(KEY_HADOOP_HOME);
        if (StringUtils.isNotEmpty(hadoopHome)) {
            hadoopHome = hadoopHome.trim();
            if (hadoopHome.endsWith(File.separator)) {
                hadoopHome = hadoopHome.substring(0, hadoopHome.lastIndexOf(File.separator));
            }

            launcherOptions.setHadoopConfDir(hadoopHome + "/etc/hadoop");
        }
    }

    private static void findDefaultFlinkConf(Options launcherOptions) {

        String flinkHome = getSystemProperty(KEY_FLINK_HOME);
        if (StringUtils.isNotEmpty(flinkHome)) {
            flinkHome = flinkHome.trim();
            if (flinkHome.endsWith(File.separator)) {
                flinkHome = flinkHome.substring(0, flinkHome.lastIndexOf(File.separator));
            }

            if (StringUtils.isEmpty(launcherOptions.getFlinkConfDir())) {
                launcherOptions.setFlinkConfDir(flinkHome + "/conf");
            }

            if (StringUtils.isEmpty(launcherOptions.getFlinkLibDir())) {
                launcherOptions.setFlinkLibDir(flinkHome + "/lib");
            }
        }
    }

    private static void findDefaultChunJunDistDir(@NotNull Options launcherOptions)
            throws ClusterDeploymentException {
        String distDir = launcherOptions.getChunjunDistDir();
        if (StringUtils.isEmpty(distDir)) {
            String chunjunHome = getSystemProperty(KEY_CHUNJUN_HOME);
            if (StringUtils.isNotEmpty(chunjunHome)) {
                chunjunHome = chunjunHome.trim();
                if (chunjunHome.endsWith(File.separator)) {
                    distDir = chunjunHome + PLUGINS_DIR_NAME;
                } else {
                    distDir = chunjunHome + File.separator + PLUGINS_DIR_NAME;
                }

                launcherOptions.setChunjunDistDir(distDir);
            }
        }
        if (StringUtils.isEmpty(distDir)) {
            throw new ClusterDeploymentException(KEY_CHUNJUN_HOME + " is not configured.");
        }
        System.setProperty(ConfigConstants.ENV_FLINK_PLUGINS_DIR, distDir);
    }

    private static String getSystemProperty(String name) {
        String property = System.getenv(name);
        if (StringUtils.isEmpty(property)) {
            return System.getProperty(name);
        }

        return property;
    }
}
