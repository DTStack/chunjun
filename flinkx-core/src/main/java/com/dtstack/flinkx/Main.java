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

import com.dtstack.flink.api.java.MyLocalStreamEnvironment;
import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.constants.ConfigConstrant;
import com.dtstack.flinkx.reader.DataReader;
import com.dtstack.flinkx.reader.DataReaderFactory;
import com.dtstack.flinkx.writer.DataWriter;
import com.dtstack.flinkx.writer.DataWriterFactory;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * The main class entry
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class Main {

    public static Logger LOG = LoggerFactory.getLogger(Main.class);

    private static final int FAILURE_RATE = 3;

    private static final int FAILURE_INTERVAL = 6;

    private static final int DELAY_INTERVAL = 10;

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        // 解析命令行参数
        Options options = new Options();
        options.addOption("job", true, "Job config.");
        options.addOption("jobid", true, "Job unique id.");
        options.addOption("monitor", true, "Monitor Addresses");
        options.addOption("pluginRoot", true, "plugin path root");
        options.addOption("confProp", true, "env properties");
        options.addOption("s", true, "savepoint path");

        BasicParser parser = new BasicParser();
        CommandLine cl = parser.parse(options, args);
        String job = cl.getOptionValue("job");
        String jobIdString = cl.getOptionValue("jobid");
        String monitor = cl.getOptionValue("monitor");
        String pluginRoot = cl.getOptionValue("pluginRoot");
        String savepointPath = cl.getOptionValue("s");
        Properties confProperties = parseConf(cl.getOptionValue("confProp"));

        Preconditions.checkNotNull(job, "Must provide --job argument");
        Preconditions.checkNotNull(jobIdString, "Must provide --jobid argument");

        // 解析jobPath指定的任务配置文件
        DataTransferConfig config = DataTransferConfig.parse(job);

        if(StringUtils.isNotEmpty(monitor)) {
            config.setMonitorUrls(monitor);
        }

        if(StringUtils.isNotEmpty(pluginRoot)) {
            config.setPluginRoot(pluginRoot);
        }

        StreamExecutionEnvironment env = (StringUtils.isNotBlank(monitor)) ?
                StreamExecutionEnvironment.getExecutionEnvironment() :
                new MyLocalStreamEnvironment();

        env = openCheckpointConf(env, confProperties);

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

            if(StringUtils.isNotEmpty(savepointPath)){
                ((MyLocalStreamEnvironment) env).setSettings(SavepointRestoreSettings.forPath(savepointPath));
            }
        }

        env.execute(jobIdString);

    }

    private static Properties parseConf(String confStr) throws Exception{
        if(StringUtils.isEmpty(confStr)){
            return new Properties();
        }

        confStr = URLDecoder.decode(confStr, Charsets.UTF_8.toString());
        return objectMapper.readValue(confStr, Properties.class);
    }

    private static StreamExecutionEnvironment openCheckpointConf(StreamExecutionEnvironment env, Properties properties){
        if(properties!=null){
            String interval = properties.getProperty(ConfigConstrant.FLINK_CHECKPOINT_INTERVAL_KEY);
            if(StringUtils.isNotBlank(interval)){
                env.enableCheckpointing(Long.valueOf(interval.trim()));
                LOG.info("Open checkpoint with interval:" + interval);
            }
            String checkpointTimeoutStr = properties.getProperty(ConfigConstrant.FLINK_CHECKPOINT_TIMEOUT_KEY);
            if(checkpointTimeoutStr != null){
                long checkpointTimeout = Long.valueOf(checkpointTimeoutStr.trim());
                //checkpoints have to complete within one min,or are discard
                env.getCheckpointConfig().setCheckpointTimeout(checkpointTimeout);

                LOG.info("Set checkpoint timeout:" + checkpointTimeout);
            }
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().enableExternalizedCheckpoints(
                    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
            String backendPath = properties.getProperty(ConfigConstrant.FLINK_CHECKPOINT_DATAURI_KEY);
            if(backendPath != null){
                //set checkpoint save path on file system,hdfs://, file://
                env.setStateBackend(new FsStateBackend(backendPath.trim()));
                LOG.info("Set StateBackend:" + backendPath);
            }
        }
        return env;
    }
}
