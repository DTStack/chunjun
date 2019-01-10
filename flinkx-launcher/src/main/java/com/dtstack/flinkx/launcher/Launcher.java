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

import com.dtstack.flinkx.config.ContentConfig;
import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.launcher.perjob.PerJobSubmitter;
import com.dtstack.flinkx.util.SysUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.Preconditions;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * FlinkX commandline Launcher
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class Launcher {

    private static List<String> initFlinkxArgList(LauncherOptions launcherOptions) {
        List<String> argList = new ArrayList<>();
        argList.add("-job");
        argList.add(launcherOptions.getJob());
        argList.add("-jobid");
        argList.add(launcherOptions.getJobid());
        argList.add("-pluginRoot");
        argList.add(launcherOptions.getPlugin());
        argList.add("-mode");
        argList.add(launcherOptions.getMode());
        return argList;
    }

    private static List<URL> analyzeUserClasspath(String content, String pluginRoot) {

        List<URL> urlList = new ArrayList<>();

        DataTransferConfig config = DataTransferConfig.parse(content);

        Preconditions.checkNotNull(pluginRoot);

        ContentConfig contentConfig = config.getJob().getContent().get(0);
        String readerName = contentConfig.getReader().getName().toLowerCase();
        File readerDir = new File(pluginRoot + File.separator + readerName);
        String writerName = contentConfig.getWriter().getName().toLowerCase();
        File writerDir = new File(pluginRoot + File.separator + writerName);
        File commonDir = new File(pluginRoot + File.separator + "common");

        try {
            urlList.addAll(SysUtil.findJarsInDir(readerDir));
            urlList.addAll(SysUtil.findJarsInDir(writerDir));
            urlList.addAll(SysUtil.findJarsInDir(commonDir));
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }

        return urlList;
    }

    public static void main(String[] args) throws Exception {
        LauncherOptions launcherOptions = new LauncherOptionParser(args).getLauncherOptions();
        List<String> argList = initFlinkxArgList(launcherOptions);
        String mode = launcherOptions.getMode();
        if(mode.equalsIgnoreCase(ClusterMode.local.name())) {
            String[] localArgs = argList.toArray(new String[argList.size()]);
            com.dtstack.flinkx.Main.main(localArgs);
        } else {
            String pluginRoot = launcherOptions.getPlugin();
            String content = launcherOptions.getJob();
            File jarFile = new File(pluginRoot + File.separator + "flinkx.jar");
            List<URL> urlList = analyzeUserClasspath(content, pluginRoot);

            if (mode.equalsIgnoreCase(ClusterMode.yarn.name())){
                ClusterClient clusterClient = ClusterClientFactory.createClusterClient(launcherOptions);
                String monitor = clusterClient.getWebInterfaceURL();
                argList.add("-monitor");
                argList.add(monitor);

                String[] remoteArgs = argList.toArray(new String[argList.size()]);
                PackagedProgram program = new PackagedProgram(jarFile, urlList, remoteArgs);
                clusterClient.run(program, launcherOptions.getParallelism());
                clusterClient.shutdown();
                System.exit(0);
            }else {
                String confProp = launcherOptions.getConfProp();
                if (!StringUtils.isNotBlank(confProp)){
                    throw new IllegalStateException("perjob mode must have confProp ");
                }

                String libJar = launcherOptions.getFlinkLibJar();
                if (!StringUtils.isNotBlank(libJar)){
                    throw new IllegalStateException("perjob mode must have flink lib path ");
                }

                String[] remoteArgs = argList.toArray(new String[argList.size()]);
                PackagedProgram program = new PackagedProgram(jarFile, urlList, remoteArgs);

                String flinkConfDir = launcherOptions.getFlinkconf();
                Configuration config = GlobalConfiguration.loadConfiguration(flinkConfDir);
                JobGraph jobGraph = PackagedProgramUtils.createJobGraph(program, config, launcherOptions.getParallelism());

                PerJobSubmitter.submit(launcherOptions, jobGraph);
            }
        }
    }
}
