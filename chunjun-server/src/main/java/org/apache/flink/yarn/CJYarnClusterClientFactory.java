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
 * 然后基于classloader.getResource("yarn-site.xml") 来构建资源配置 Company: www.dtstack.com
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
