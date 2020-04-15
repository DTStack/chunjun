/*
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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.dtstack.flinkx.config.ContentConfig;
import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.enums.ClusterMode;
import com.dtstack.flinkx.launcher.perjob.PerJobSubmitter;
import com.dtstack.flinkx.options.OptionParser;
import com.dtstack.flinkx.options.Options;
import com.dtstack.flinkx.util.SysUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.util.Preconditions;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * FlinkX commandline Launcher
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class Launcher {

    public static final String KEY_FLINKX_HOME = "FLINKX_HOME";
    public static final String KEY_FLINK_HOME = "FLINK_HOME";
    public static final String KEY_HADOOP_HOME = "HADOOP_HOME";

    public static final String PLUGINS_DIR_NAME = "plugins";

    public static final String CORE_JAR_NAME_PREFIX = "flinkx";

    private static List<URL> analyzeUserClasspath(String content, String pluginRoot) {

        List<URL> urlList = new ArrayList<>();

        String jobJson = readJob(content);
        DataTransferConfig config = DataTransferConfig.parse(jobJson);

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
        setLogLevel(Level.INFO.toString());
        OptionParser optionParser = new OptionParser(args);
        Options launcherOptions = optionParser.getOptions();
        findDefaultConfigDir(launcherOptions);

        String mode = launcherOptions.getMode();
        List<String> argList = optionParser.getProgramExeArgList();
        if(mode.equals(ClusterMode.local.name())) {
            String[] localArgs = argList.toArray(new String[argList.size()]);
            com.dtstack.flinkx.Main.main(localArgs);
        } else {
            String pluginRoot = launcherOptions.getPluginRoot();
            String content = launcherOptions.getJob();
            String coreJarName = getCoreJarFileName(pluginRoot);
            File jarFile = new File(pluginRoot + File.separator + coreJarName);
            List<URL> urlList = analyzeUserClasspath(content, pluginRoot);
            if(!ClusterMode.yarnPer.name().equals(mode)){
                ClusterClient clusterClient = ClusterClientFactory.createClusterClient(launcherOptions);
                String monitor = clusterClient.getWebInterfaceURL();
                argList.add("-monitor");
                argList.add(monitor);

                String[] remoteArgs = argList.toArray(new String[0]);

                ClassLoaderType classLoaderType = ClassLoaderType.getByClassMode(launcherOptions.getPluginLoadMode());
                PackagedProgram program = new PackagedProgram(jarFile, urlList, classLoaderType, "com.dtstack.flinkx.Main", remoteArgs);

                if (StringUtils.isNotEmpty(launcherOptions.getS())){
                    program.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(launcherOptions.getS()));
                }

                clusterClient.run(program, Integer.parseInt(launcherOptions.getParallelism()));
                clusterClient.shutdown();
            }else{
                String confProp = launcherOptions.getConfProp();
                if (StringUtils.isBlank(confProp)){
                    throw new IllegalArgumentException("per-job mode must have confProp!");
                }

                String libJar = launcherOptions.getFlinkLibJar();
                if (StringUtils.isBlank(libJar)){
                    throw new IllegalArgumentException("per-job mode must have flink lib path!");
                }

                argList.add("-monitor");
                argList.add("");

                //jdk内在优化，使用空数组效率更高
                String[] remoteArgs = argList.toArray(new String[0]);
                PerJobSubmitter.submit(launcherOptions, jarFile, remoteArgs);
            }
        }
    }

    private static void findDefaultConfigDir(Options launcherOptions) {
        findDefaultPluginRoot(launcherOptions);

        if (ClusterMode.local.name().equalsIgnoreCase(launcherOptions.getMode())) {
            return;
        }

        findDefaultFlinkConf(launcherOptions);
        findDefaultHadoopConf(launcherOptions);
    }

    private static void findDefaultHadoopConf(Options launcherOptions) {
        if (StringUtils.isNotEmpty(launcherOptions.getYarnconf())) {
            return;
        }

        String hadoopHome = getSystemProperty(KEY_HADOOP_HOME);
        if (StringUtils.isNotEmpty(hadoopHome)) {
            hadoopHome = hadoopHome.trim();
            if (hadoopHome.endsWith(File.separator)) {
                hadoopHome = hadoopHome.substring(0, hadoopHome.lastIndexOf(File.separator));
            }

            launcherOptions.setYarnconf(hadoopHome + "/etc/hadoop");
        }
    }

    private static void findDefaultFlinkConf(Options launcherOptions) {
        if (StringUtils.isNotEmpty(launcherOptions.getFlinkconf()) && StringUtils.isNotEmpty(launcherOptions.getFlinkLibJar())) {
            return;
        }

        String flinkHome = getSystemProperty(KEY_FLINK_HOME);
        if (StringUtils.isNotEmpty(flinkHome)) {
            flinkHome = flinkHome.trim();
            if (flinkHome.endsWith(File.separator)){
                flinkHome = flinkHome.substring(0, flinkHome.lastIndexOf(File.separator));
            }

            launcherOptions.setFlinkconf(flinkHome + "/conf");
            launcherOptions.setFlinkLibJar(flinkHome + "/lib");
        }
    }

    private static void findDefaultPluginRoot(Options launcherOptions) {
        String pluginRoot = launcherOptions.getPluginRoot();
        if (StringUtils.isNotEmpty(pluginRoot)) {
            return;
        }

        String flinkxHome = getSystemProperty(KEY_FLINKX_HOME);
        if (StringUtils.isNotEmpty(flinkxHome)) {
            flinkxHome = flinkxHome.trim();
            if (flinkxHome.endsWith(File.separator)) {
                pluginRoot = flinkxHome + PLUGINS_DIR_NAME;
            } else {
                pluginRoot = flinkxHome + File.separator + PLUGINS_DIR_NAME;
            }

            launcherOptions.setPluginRoot(pluginRoot);
        }
    }

    private static String getSystemProperty(String name) {
        String property = System.getenv(name);
        if (StringUtils.isEmpty(property)) {
            property = System.getProperty(name);
        }

        return property;
    }

    private static String getCoreJarFileName (String pluginRoot) throws FileNotFoundException{
        String coreJarFileName = null;
        File pluginDir = new File(pluginRoot);
        if (pluginDir.exists() && pluginDir.isDirectory()){
            File[] jarFiles = pluginDir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.toLowerCase().startsWith(CORE_JAR_NAME_PREFIX) && name.toLowerCase().endsWith(".jar");
                }
            });

            if (jarFiles != null && jarFiles.length > 0){
                coreJarFileName = jarFiles[0].getName();
            }
        }

        if (StringUtils.isEmpty(coreJarFileName)){
            throw new FileNotFoundException("Can not find core jar file in path:" + pluginRoot);
        }

        return coreJarFileName;
    }

    private static String readJob(String job) {
        try {
            File file = new File(job);
            FileInputStream in = new FileInputStream(file);
            byte[] fileContent = new byte[(int) file.length()];
            in.read(fileContent);
            in.close();
            return new String(fileContent, StandardCharsets.UTF_8);
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    private static void setLogLevel(String level){
        LoggerContext loggerContext= (LoggerContext) LoggerFactory.getILoggerFactory();
        //设置全局日志级别
        ch.qos.logback.classic.Logger logger=loggerContext.getLogger("root");
        logger.setLevel(Level.toLevel(level));
    }
}
