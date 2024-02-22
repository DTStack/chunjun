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
package org.apache.flink.yarn;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptionsInternal;
import org.apache.flink.yarn.configuration.YarnLogConfigUtil;

import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * 调整org.apache.flink.yarn.YarnClusterClientFactory.getClusterDescriptor 逻辑添加对yarnConf 的设定 flink
 * 源码中客户端构建yarnConf 依赖bin
 * 脚本里的环境变量的设定INTERNAL_HADOOP_CLASSPATHS="${HADOOP_CLASSPATH}:${HADOOP_CONF_DIR}:${YARN_CONF_DIR}"
 * 然后基于classloader.getResource("yarn-site.xml") 来构建资源配置
 *
 * @author xuchao
 * @date 2023-07-13
 */
public class CJYarnClusterClientFactory extends YarnClusterClientFactory {

    public YarnClusterDescriptor createClusterDescriptor(
            Configuration configuration, YarnConfiguration yarnConfiguration) {
        checkNotNull(configuration);

        final String configurationDirectory = configuration.get(DeploymentOptionsInternal.CONF_DIR);
        YarnLogConfigUtil.setLogConfigFileInConfig(configuration, configurationDirectory);

        return getClusterDescriptor(configuration, yarnConfiguration);
    }

    private YarnClusterDescriptor getClusterDescriptor(
            Configuration configuration, YarnConfiguration yarnConfiguration) {
        final YarnClient yarnClient = YarnClient.createYarnClient();

        yarnClient.init(yarnConfiguration);
        yarnClient.start();

        return new YarnClusterDescriptor(
                configuration,
                yarnConfiguration,
                yarnClient,
                YarnClientYarnClusterInformationRetriever.create(yarnClient),
                false);
    }
}
