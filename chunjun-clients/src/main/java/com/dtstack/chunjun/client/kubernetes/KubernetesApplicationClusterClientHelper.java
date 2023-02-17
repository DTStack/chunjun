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
package com.dtstack.chunjun.client.kubernetes;

import com.dtstack.chunjun.client.ClusterClientHelper;
import com.dtstack.chunjun.client.JobDeployer;
import com.dtstack.chunjun.client.util.PluginInfoUtil;
import com.dtstack.chunjun.options.Options;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.DeploymentOptionsInternal;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.KubernetesClusterClientFactory;
import org.apache.flink.kubernetes.KubernetesClusterDescriptor;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.jobmanager.JobManagerProcessUtils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

@Slf4j
public class KubernetesApplicationClusterClientHelper implements ClusterClientHelper<String> {

    public static final String KUBERNETES_HOST_ALIASES_ENV = "KUBERNETES_HOST_ALIASES";

    @Override
    public ClusterClient<String> submit(JobDeployer jobDeployer) throws Exception {
        Options launcherOptions = jobDeployer.getLauncherOptions();
        List<String> programArgs = jobDeployer.getProgramArgs();
        Configuration effectiveConfiguration = jobDeployer.getEffectiveConfiguration();

        setDeployerConfig(effectiveConfiguration, launcherOptions);

        // set host aliases
        setHostAliases(effectiveConfiguration);

        replaceRemoteParams(programArgs, effectiveConfiguration);

        ApplicationConfiguration applicationConfiguration =
                new ApplicationConfiguration(
                        programArgs.toArray(new String[0]), PluginInfoUtil.getMainClass());

        KubernetesClusterClientFactory kubernetesClusterClientFactory =
                new KubernetesClusterClientFactory();
        try (KubernetesClusterDescriptor descriptor =
                kubernetesClusterClientFactory.createClusterDescriptor(effectiveConfiguration)) {
            ClusterSpecification clusterSpecification =
                    getClusterSpecification(effectiveConfiguration);
            ClusterClientProvider<String> clientProvider =
                    descriptor.deployApplicationCluster(
                            clusterSpecification, applicationConfiguration);
            ClusterClient<String> clusterClient = clientProvider.getClusterClient();
            log.info("Deploy Application with Cluster Id: {}", clusterClient.getClusterId());
            return clusterClient;
        }
    }

    private ClusterSpecification getClusterSpecification(Configuration configuration) {
        checkNotNull(configuration);

        final int jobManagerMemoryMB =
                JobManagerProcessUtils.processSpecFromConfigWithNewOptionToInterpretLegacyHeap(
                                configuration, JobManagerOptions.TOTAL_PROCESS_MEMORY)
                        .getTotalProcessMemorySize()
                        .getMebiBytes();

        final int taskManagerMemoryMB =
                TaskExecutorProcessUtils.processSpecFromConfig(
                                TaskExecutorProcessUtils
                                        .getConfigurationMapLegacyTaskManagerHeapSizeToConfigOption(
                                                configuration,
                                                TaskManagerOptions.TOTAL_PROCESS_MEMORY))
                        .getTotalProcessMemorySize()
                        .getMebiBytes();

        int slotsPerTaskManager = configuration.getInteger(TaskManagerOptions.NUM_TASK_SLOTS);

        return new ClusterSpecification.ClusterSpecificationBuilder()
                .setMasterMemoryMB(jobManagerMemoryMB)
                .setTaskManagerMemoryMB(taskManagerMemoryMB)
                .setSlotsPerTaskManager(slotsPerTaskManager)
                .createClusterSpecification();
    }

    private void replaceRemoteParams(List<String> programArgs, Configuration flinkConfig) {

        HashMap<String, String> temp = new HashMap<>(16);
        for (int i = 0; i < programArgs.size(); i += 2) {
            if (StringUtils.equalsIgnoreCase(programArgs.get(i), "-flinkConfDir")) {
                temp.put(
                        programArgs.get(i),
                        flinkConfig.getString(KubernetesConfigOptions.FLINK_CONF_DIR));
            } else if (StringUtils.equalsIgnoreCase(programArgs.get(i), "-job")) {
                String jobContent = programArgs.get(i + 1);
                String newJobContent =
                        StringUtils.replace(jobContent, System.getProperty("line.separator"), " ");
                temp.put(programArgs.get(i), newJobContent);
            } else {
                temp.put(programArgs.get(i), programArgs.get(i + 1));
            }
        }

        // 清空list，填充修改后的参数值
        programArgs.clear();
        for (int i = 0; i < temp.size(); i++) {
            programArgs.add(temp.keySet().toArray()[i].toString());
            programArgs.add(temp.values().toArray()[i].toString());
        }
    }

    private void setDeployerConfig(Configuration configuration, Options launcherOptions)
            throws FileNotFoundException {
        configuration.set(DeploymentOptionsInternal.CONF_DIR, launcherOptions.getFlinkConfDir());

        String coreJarFileName =
                PluginInfoUtil.getCoreJarName(launcherOptions.getChunjunDistDir())
                        .orElseThrow(
                                () ->
                                        new FileNotFoundException(
                                                "Can not find core jar file in path:"
                                                        + launcherOptions.getChunjunDistDir()));
        String remoteCoreJarPath =
                "local://"
                        + launcherOptions.getRemoteChunJunDistDir()
                        + File.separator
                        + coreJarFileName;
        configuration.set(PipelineOptions.JARS, Collections.singletonList(remoteCoreJarPath));

        configuration.set(
                DeploymentOptions.TARGET, KubernetesDeploymentTarget.APPLICATION.getName());

        configuration.setString(CoreOptions.CLASSLOADER_RESOLVE_ORDER, "parent-first");
    }

    private void setHostAliases(Configuration flinkConfig) {
        String hostAliases = flinkConfig.getString(DTConfigurationOptions.KUBERNETES_HOST_ALIASES);
        if (StringUtils.isNotBlank(hostAliases)) {
            flinkConfig.setString(buildMasterEnvKey(), hostAliases);
            flinkConfig.setString(buildTaskManagerEnvKey(), hostAliases);
        }
    }

    private String buildMasterEnvKey() {
        return ResourceManagerOptions.CONTAINERIZED_MASTER_ENV_PREFIX + KUBERNETES_HOST_ALIASES_ENV;
    }

    private String buildTaskManagerEnvKey() {
        return ResourceManagerOptions.CONTAINERIZED_TASK_MANAGER_ENV_PREFIX
                + KUBERNETES_HOST_ALIASES_ENV;
    }
}
