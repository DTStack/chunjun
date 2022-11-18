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
package com.dtstack.chunjun.client.util;

import com.dtstack.chunjun.options.Options;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.CoreOptions.DEFAULT_PARALLELISM;

@Slf4j
public class JobGraphUtil {

    public static JobGraph buildJobGraph(Options launcherOptions, String[] programArgs)
            throws Exception {
        String pluginRoot = launcherOptions.getChunjunDistDir();
        String coreJarPath = PluginInfoUtil.getCoreJarPath(pluginRoot);
        File jarFile = new File(coreJarPath);
        Configuration flinkConf = launcherOptions.loadFlinkConfiguration();
        PackagedProgram program =
                PackagedProgram.newBuilder()
                        .setJarFile(jarFile)
                        .setUserClassPaths(Lists.newArrayList(getURLFromRootDir(pluginRoot)))
                        .setEntryPointClassName(PluginInfoUtil.getMainClass())
                        .setConfiguration(launcherOptions.loadFlinkConfiguration())
                        .setArguments(programArgs)
                        .build();
        JobGraph jobGraph =
                PackagedProgramUtils.createJobGraph(
                        program,
                        launcherOptions.loadFlinkConfiguration(),
                        flinkConf.getInteger(DEFAULT_PARALLELISM),
                        false);
        List<URL> pluginClassPath =
                jobGraph.getUserArtifacts().entrySet().stream()
                        .filter(tmp -> tmp.getKey().startsWith("class_path"))
                        .map(tmp -> new File(tmp.getValue().filePath))
                        .map(
                                file -> {
                                    try {
                                        return file.toURI().toURL();
                                    } catch (MalformedURLException e) {
                                        log.error(e.getMessage());
                                    }
                                    return null;
                                })
                        .collect(Collectors.toList());
        jobGraph.setClasspaths(pluginClassPath);
        return jobGraph;
    }

    public static Set<URL> getURLFromRootDir(String path) {
        Set<URL> urlSet = Sets.newHashSet();
        File plugins = new File(path);
        if (!plugins.exists()) {
            throw new ChunJunRuntimeException(
                    path + " is not exist! Please check the configuration.");
        }

        addFileToURL(urlSet, Optional.of(plugins));
        return urlSet;
    }

    public static void addFileToURL(Set<URL> urlSet, Optional<File> pluginFile) {
        pluginFile.ifPresent(
                item -> {
                    File[] files = item.listFiles();
                    assert files != null;
                    for (File file : files) {
                        if (file.isDirectory()) {
                            addFileToURL(urlSet, Optional.of(file));
                        }

                        if (file.isFile() && file.isAbsolute()) {
                            try {
                                urlSet.add(file.toURI().toURL());
                            } catch (MalformedURLException e) {
                                throw new ChunJunRuntimeException(
                                        "The error should not occur, please check the code.", e);
                            }
                        }
                    }
                });
    }

    public static PackagedProgram buildProgram(ClusterSpecification clusterSpecification)
            throws Exception {
        String[] args = clusterSpecification.getProgramArgs();
        return PackagedProgram.newBuilder()
                .setJarFile(clusterSpecification.getJarFile())
                .setUserClassPaths(clusterSpecification.getClasspaths())
                .setEntryPointClassName(clusterSpecification.getEntryPointClass())
                .setConfiguration(clusterSpecification.getConfiguration())
                .setSavepointRestoreSettings(clusterSpecification.getSpSetting())
                .setArguments(args)
                .build();
    }
}
