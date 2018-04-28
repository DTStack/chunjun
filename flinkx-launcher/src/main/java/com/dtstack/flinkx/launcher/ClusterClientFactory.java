/**
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

package com.dtstack.flinkx.launcher;

import org.apache.flink.client.deployment.StandaloneClusterDescriptor;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.StandaloneClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.yarn.YarnClusterClient;
import java.util.Properties;
import static com.dtstack.flinkx.launcher.LauncherOptions.OPTION_MODE;

/**
 * The Factory of ClusterClient
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class ClusterClientFactory {

    public static ClusterClient createClusterClient(Properties props) {
        String clientType = props.getProperty(OPTION_MODE);
        if(clientType.equals(ClusterMode.MODE_STANDALONE)) {
            return createStandaloneClient(props);
        } else if(clientType.equals(ClusterMode.MODE_YARN)) {
            return createYarnClient(props);
        }
        throw new IllegalArgumentException("Unsupported cluster client type: ");
    }

    public static StandaloneClusterClient createStandaloneClient(Properties props) {
        String flinkConfDir = props.getProperty(LauncherOptions.OPTION_FLINK_CONF_DIR);
        Configuration config = GlobalConfiguration.loadConfiguration(flinkConfDir);
        StandaloneClusterDescriptor descriptor = new StandaloneClusterDescriptor(config);
        StandaloneClusterClient clusterClient = descriptor.retrieve(null);
        clusterClient.setDetached(true);
        return clusterClient;
    }

    public static YarnClusterClient createYarnClient(Properties props) {
        throw new UnsupportedOperationException("Haven't been developed yet!");
    }

}
