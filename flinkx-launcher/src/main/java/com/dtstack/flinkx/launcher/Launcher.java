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
import com.dtstack.flinkx.util.StringUtil;
import com.dtstack.flinkx.util.SysUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.util.Preconditions;
import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
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

        if (StringUtils.isNotEmpty(launcherOptions.getConfProp())){
            argList.add("-confProp");
            argList.add(launcherOptions.getConfProp());
        }

        if(StringUtils.isNotEmpty(launcherOptions.getSavepoint())){
            argList.add("-s");
            argList.add(launcherOptions.getSavepoint());
        }

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
        String mode = launcherOptions.getMode();
        List<String> argList = initFlinkxArgList(launcherOptions);
        if(mode.equals(ClusterMode.local.name())) {
            String[] localArgs = argList.toArray(new String[argList.size()]);
            com.dtstack.flinkx.Main.main(localArgs);
        } else {
            ClusterClient clusterClient = ClusterClientFactory.createClusterClient(launcherOptions);
            String monitor = clusterClient.getWebInterfaceURL();
            argList.add("-monitor");
            argList.add(monitor);

            String pluginRoot = launcherOptions.getPlugin();
            String content = launcherOptions.getJob();
            String coreJarName = getCoreJarName(pluginRoot);
            File jarFile = new File(pluginRoot + File.separator + coreJarName);
            List<URL> urlList = analyzeUserClasspath(content, pluginRoot);
            String[] remoteArgs = argList.toArray(new String[argList.size()]);
            PackagedProgram program = new PackagedProgram(jarFile, urlList, remoteArgs);

            if (StringUtils.isNotEmpty(launcherOptions.getSavepoint())){
                program.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(launcherOptions.getSavepoint()));
            }

            clusterClient.run(program, launcherOptions.getParallelism());
            clusterClient.shutdown();
        }
    }

    private static String getCoreJarName(String pluginRoot){
        String coreJarName = null;

        File rootDir = new File(pluginRoot);
        if (!rootDir.exists()){
            throw new RuntimeException("plugin dir dose not exists");
        }

        if(rootDir.isFile()){
            throw new RuntimeException("plugin dir must be dir");
        }

        String[] files = rootDir.list((dir, name) -> name.startsWith("flinkx-") && name.endsWith(".jar"));

        if (files != null && files.length > 0){
            coreJarName = files[0];
        }

        if (coreJarName == null){
            throw new RuntimeException("Can not find core jar from plugin dir");
        }

        return coreJarName;
    }
}
