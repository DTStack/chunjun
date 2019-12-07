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
package com.dtstack.flinkx.launcher.perJob;

import com.dtstack.flinkx.launcher.YarnConfLoader;
import com.dtstack.flinkx.options.Options;
import com.google.common.base.Strings;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

/**
 * Date: 2019/09/11
 * Company: www.dtstack.com
 * @author tudou
 */
public class PerJobClusterClientBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(PerJobClusterClientBuilder.class);

    private YarnClient yarnClient;
    private YarnConfiguration yarnConf;

    /**
     * init yarnClient
     * @param yarnConfDir the path of yarnconf
     */
    public void init(String yarnConfDir) {
        if (Strings.isNullOrEmpty(yarnConfDir)) {
            throw new RuntimeException("param:[yarnconf] is required !");
        }
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

        Configuration conf = new Configuration();
        confProp.forEach((key, value) -> conf.setString(key.toString(), value.toString()));

        AbstractYarnClusterDescriptor descriptor = new YarnClusterDescriptor(conf, yarnConf, options.getFlinkconf(), yarnClient, false);
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
        if (StringUtils.equalsIgnoreCase(options.getPluginLoadMode(), "shipfile")) {
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
        descriptor.addShipFiles(shipFiles);
        return descriptor;
    }
}