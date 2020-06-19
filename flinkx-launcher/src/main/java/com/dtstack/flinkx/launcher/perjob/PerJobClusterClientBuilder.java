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
package com.dtstack.flinkx.launcher.perjob;

import com.dtstack.flinkx.launcher.YarnConfLoader;
import com.dtstack.flinkx.options.Options;
import com.google.common.base.Strings;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.cli.FlinkYarnSessionCli;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Date: 2019/09/11
 * Company: www.dtstack.com
 * @author tudou
 */
public class PerJobClusterClientBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(PerJobClusterClientBuilder.class);

    private static final String CLASS_LOAD_MODE_SHIP_FILE = "shipfile";

    private YarnClient yarnClient;

    private YarnConfiguration yarnConf;

    private Configuration flinkConfig;

    /**
     * init yarnClient
     * @param yarnConfDir the path of yarnconf
     */
    public void init(String yarnConfDir, Configuration flinkConfig, Properties userConf) throws Exception {

        if(Strings.isNullOrEmpty(yarnConfDir)) {
            throw new RuntimeException("parameters of yarn is required");
        }
        userConf.forEach((key, val) -> flinkConfig.setString(key.toString(), val.toString()));
        this.flinkConfig = flinkConfig;
        SecurityUtils.install(new SecurityConfiguration(flinkConfig));

        yarnConf = YarnConfLoader.getYarnConf(yarnConfDir);
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConf);
        yarnClient.start();

        LOG.info("----init yarn success ----");
    }

    /**
     * create a yarn cluster descriptor which is used to start the application master
     * @param confProp taskParams
     * @param options LauncherOptions
     * @param jobGraph JobGraph
     * @return
     * @throws MalformedURLException
     */
    public AbstractYarnClusterDescriptor createPerJobClusterDescriptor(Properties confProp, Options options, JobGraph jobGraph) throws MalformedURLException {
        String flinkJarPath = options.getFlinkLibJar();
        if (StringUtils.isNotBlank(flinkJarPath)) {
            if (!new File(flinkJarPath).exists()) {
                throw new IllegalArgumentException("The Flink jar path is not exist");
            }
        } else {
            throw new IllegalArgumentException("The Flink jar path is null");
        }

        AbstractYarnClusterDescriptor descriptor = new YarnClusterDescriptor(flinkConfig, yarnConf, options.getFlinkconf(), yarnClient, false);
        List<File> shipFiles = new ArrayList<>();
        File[] jars = new File(flinkJarPath).listFiles();
        if (jars != null) {
            for (File jar : jars) {
                if (jar.toURI().toURL().toString().contains("flink-dist")) {
                    descriptor.setLocalJarPath(new Path(jar.toURI().toURL().toString()));
                } else {
                    shipFiles.add(jar);
                }
            }
        }

        if (StringUtils.equalsIgnoreCase(options.getPluginLoadMode(), CLASS_LOAD_MODE_SHIP_FILE)) {
            Map<String, DistributedCache.DistributedCacheEntry> jobCacheFileConfig = jobGraph.getUserArtifacts();
            for(Map.Entry<String,  DistributedCache.DistributedCacheEntry> tmp : jobCacheFileConfig.entrySet()){
                if(tmp.getKey().startsWith("class_path")){
                    shipFiles.add(new File(tmp.getValue().filePath));
                }
            }
        }

        if (StringUtils.isNotBlank(options.getQueue())) {
            descriptor.setQueue(options.getQueue());
        }

        File log4j = new File(options.getFlinkconf()+ File.separator + FlinkYarnSessionCli.CONFIG_FILE_LOG4J_NAME);
        if(log4j.exists()){
            shipFiles.add(log4j);
        }else{
            File logback = new File(options.getFlinkconf()+ File.separator + FlinkYarnSessionCli.CONFIG_FILE_LOGBACK_NAME);
            if(logback.exists()){
                shipFiles.add(logback);
            }
        }
        descriptor.addShipFiles(shipFiles);
        return descriptor;
    }
}