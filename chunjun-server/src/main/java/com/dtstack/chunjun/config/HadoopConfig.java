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

import com.dtstack.chunjun.yarn.HadoopConfTool;
import com.dtstack.chunjun.yarn.YarnConfLoader;

import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author xuchao
 * @date 2023-05-22
 */
public class HadoopConfig {

    private static final Logger LOG = LoggerFactory.getLogger(HadoopConfig.class);

    private Configuration hadoopConfiguration;

    private YarnConfiguration yarnConfiguration;

    private String hadoopConfDir;

    public HadoopConfig(String hadoopConfDir) {
        this.hadoopConfDir = hadoopConfDir;
    }

    public void initHadoopConf(Map<String, Object> conf) {
        hadoopConfiguration = HadoopConfTool.loadConf(hadoopConfDir);
        HadoopConfTool.setFsHdfsImplDisableCache(hadoopConfiguration);

        // replace param
        if (MapUtils.isNotEmpty(conf)) {}
    }

    public void initYarnConf(Map<String, Object> conf) {
        yarnConfiguration = YarnConfLoader.loadConf(hadoopConfDir);

        // replace param
        if (MapUtils.isNotEmpty(conf)) {}

        HadoopConfTool.replaceDefaultParam(yarnConfiguration, conf);
        LOG.info("load yarn config success");
    }

    public String getDefaultFS() {
        return hadoopConfiguration.get("fs.defaultFS");
    }

    public Configuration getHadoopConfiguration() {
        return hadoopConfiguration;
    }

    public YarnConfiguration getYarnConfiguration() {
        return yarnConfiguration;
    }
}
