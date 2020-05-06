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

import com.dtstack.flinkx.options.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
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
import java.util.Properties;

/**
 * Date: 2019/09/11
 * Company: www.dtstack.com
 * @author tudou
 */
public class PerJobClusterClientBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(PerJobClusterClientBuilder.class);

    private YarnClient yarnClient;

    private YarnConfiguration yarnConf;

    private Configuration flinkConfig;

    /**
     * init yarnClient
     */
    public void init(YarnConfiguration yarnConf, Configuration flinkConfig, Properties conProp) throws Exception {
        conProp.forEach((key, val) -> flinkConfig.setString(key.toString(), val.toString()));
        SecurityUtils.install(new SecurityConfiguration(flinkConfig));

        this.yarnConf = yarnConf;
        this.flinkConfig = flinkConfig;
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConf);
        yarnClient.start();

        LOG.info("----init yarn success ----");
    }

    /**
     * create a yarn cluster descriptor which is used to start the application master
     * @param options LauncherOptions
     * @return
     * @throws MalformedURLException
     */
    public AbstractYarnClusterDescriptor createPerJobClusterDescriptor(Options options) throws MalformedURLException {
        String flinkJarPath = options.getFlinkLibJar();
        if (StringUtils.isNotBlank(flinkJarPath)) {
            if (!new File(flinkJarPath).exists()) {
                throw new IllegalArgumentException("The Flink jar path is not exist");
            }
        } else {
            throw new IllegalArgumentException("The Flink jar path is null");
        }

        AbstractYarnClusterDescriptor descriptor = new YarnClusterDescriptor(flinkConfig, yarnConf, options.getFlinkconf(), yarnClient, false);
        descriptor.setName(options.getJobid());

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