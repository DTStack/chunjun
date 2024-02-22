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
package com.dtstack.chunjun.server.util;

import com.dtstack.chunjun.config.SessionConfig;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 基于json info 构建 JobGraph
 *
 * @author xuchao
 * @date 2023-09-21
 */
public class JobGraphBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(JobGraphBuilder.class);

    private static final String MAIN_CLASS = "com.dtstack.chunjun.Main";

    private static final String CORE_JAR_NAME_PREFIX = "chunjun";

    private SessionConfig config;

    public JobGraphBuilder(SessionConfig chunJunConfig) {
        this.config = chunJunConfig;
    }

    public JobGraph buildJobGraph(String[] programArgs) throws Exception {
        String pluginRoot = config.getChunJunLibDir();
        String coreJarPath = getCoreJarPath(pluginRoot);
        File jarFile = new File(coreJarPath);
        PackagedProgram program =
                PackagedProgram.newBuilder()
                        .setJarFile(jarFile)
                        .setUserClassPaths(Lists.newArrayList(getURLFromRootDir(pluginRoot)))
                        .setEntryPointClassName(MAIN_CLASS)
                        //
                        // .setConfiguration(launcherOptions.loadFlinkConfiguration())
                        .setArguments(programArgs)
                        .build();
        JobGraph jobGraph =
                PackagedProgramUtils.createJobGraph(program, new Configuration(), 1, false);
        List<URL> pluginClassPath =
                jobGraph.getUserArtifacts().entrySet().stream()
                        .filter(tmp -> tmp.getKey().startsWith("class_path"))
                        .map(tmp -> new File(tmp.getValue().filePath))
                        .map(
                                file -> {
                                    try {
                                        return file.toURI().toURL();
                                    } catch (MalformedURLException e) {
                                        LOG.error(e.getMessage());
                                    }
                                    return null;
                                })
                        .collect(Collectors.toList());
        jobGraph.setClasspaths(pluginClassPath);
        return jobGraph;
    }

    public String getCoreJarPath(String pluginRoot) {
        File pluginDir = new File(pluginRoot);
        if (pluginDir.exists() && pluginDir.isDirectory()) {
            File[] jarFiles =
                    pluginDir.listFiles(
                            (dir, name) ->
                                    name.toLowerCase().startsWith(CORE_JAR_NAME_PREFIX)
                                            && name.toLowerCase().endsWith(".jar"));

            if (jarFiles != null && jarFiles.length > 0) {
                return pluginRoot + File.separator + jarFiles[0].getName();
            }
        }

        throw new RuntimeException(
                String.format(
                        "can't find chunjun core file(name: chunjun*.jar) from chunjun dir %s.",
                        pluginRoot));
    }

    public Set<URL> getURLFromRootDir(String path) {
        Set<URL> urlSet = Sets.newHashSet();
        File plugins = new File(path);
        if (!plugins.exists()) {
            throw new ChunJunRuntimeException(
                    path + " is not exist! Please check the configuration.");
        }

        addFileToURL(urlSet, Optional.of(plugins));
        return urlSet;
    }

    public void addFileToURL(Set<URL> urlSet, Optional<File> pluginFile) {
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
}
