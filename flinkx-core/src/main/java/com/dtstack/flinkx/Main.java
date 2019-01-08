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

package com.dtstack.flinkx;

import com.dtstack.ClusterMode;
import com.dtstack.flink.api.java.MyLocalStreamEnvironment;
import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.reader.DataReader;
import com.dtstack.flinkx.reader.DataReaderFactory;
import com.dtstack.flinkx.writer.DataWriter;
import com.dtstack.flinkx.writer.DataWriterFactory;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

/**
 * The main class entry
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class Main {

    public static void main(String[] args) throws Exception {
        // 解析命令行参数
        Options options = new Options();
        options.addOption("mode", true, "Job run mode");
        options.addOption("job", true, "Job config.");
        options.addOption("jobid", true, "Job unique id.");
        options.addOption("pluginRoot", true, "plugin path root");
        options.addOption("yarnConf", true, "yarn conf string");

        BasicParser parser = new BasicParser();
        CommandLine cl = parser.parse(options, args);
        String job = cl.getOptionValue("job");
        String mode=cl.getOptionValue("mode");
        String jobIdString = cl.getOptionValue("jobid");
        String pluginRoot = cl.getOptionValue("pluginRoot");
        String yarnConfStr = cl.getOptionValue("yarnConf");
        Preconditions.checkNotNull(job, "Must provide --job argument");
        Preconditions.checkNotNull(jobIdString, "Must provide --jobid argument");

        // 解析jobPath指定的任务配置文件
        DataTransferConfig config = DataTransferConfig.parse(job);

        if(yarnConfStr != null && yarnConfStr.length() > 0){
            config.setMonitorUrls(getMonitorUrl(yarnConfStr,jobIdString,mode));
        }

        if(StringUtils.isNotEmpty(pluginRoot)) {
            config.setPluginRoot(pluginRoot);
        }

        //构造并执行flink任务
        StreamExecutionEnvironment env = !ClusterMode.local.name().equals(mode) ?
                StreamExecutionEnvironment.getExecutionEnvironment() :
                new MyLocalStreamEnvironment();

        env.setParallelism(config.getJob().getSetting().getSpeed().getChannel());
        env.setRestartStrategy(RestartStrategies.noRestart());
        DataReader dataReader = DataReaderFactory.getDataReader(config, env);
        DataStream<Row> dataStream = dataReader.readData();
        dataStream = dataStream.rebalance();
        DataWriter dataWriter = DataWriterFactory.getDataWriter(config);
        dataWriter.writeData(dataStream);

        if(env instanceof MyLocalStreamEnvironment) {
            List<URL> urlList = new ArrayList<>();
            URLClassLoader readerClassLoader = (URLClassLoader) dataReader.getClass().getClassLoader();
            urlList.addAll(Arrays.asList(readerClassLoader.getURLs()));
            URLClassLoader writerClassLoader = (URLClassLoader) dataWriter.getClass().getClassLoader();
            for (URL url : writerClassLoader.getURLs()) {
                if (!urlList.contains(url)) {
                    urlList.add(url);
                }
            }
            ((MyLocalStreamEnvironment) env).setClasspaths(urlList);
        }

        env.execute(jobIdString);

    }

    private static String getMonitorUrl(String yarnConfStr,String jobName,String mode) throws YarnException,IOException {
        Configuration cfg = new Configuration();
        JsonParser jsonParser = new JsonParser();
        JsonObject jsonObject = jsonParser.parse(yarnConfStr).getAsJsonObject();
        for (Map.Entry<String, JsonElement> keyVal : jsonObject.entrySet()) {
            cfg.set(keyVal.getKey(),keyVal.getValue().getAsString());
        }

        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(new YarnConfiguration(cfg));
        yarnClient.start();

        Set<String> set = new HashSet<>();
        set.add("Apache Flink");
        EnumSet<YarnApplicationState> enumSet = EnumSet.noneOf(YarnApplicationState.class);
        enumSet.add(YarnApplicationState.RUNNING);

        List<ApplicationReport> apps = yarnClient.getApplications(set,enumSet);
        for (ApplicationReport app : apps) {
            if(ClusterMode.yarnPer.name().equalsIgnoreCase(mode)){
                if(app.getName().equals(jobName)){
                    return app.getTrackingUrl();
                }
            } else if(ClusterMode.yarn.name().equalsIgnoreCase(mode)){
                if(app.getName().startsWith("Flink session")){
                    return app.getTrackingUrl();
                }
            }
        }

        return null;
    }
}
