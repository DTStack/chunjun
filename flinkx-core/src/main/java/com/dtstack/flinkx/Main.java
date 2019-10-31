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
import com.dtstack.flinkx.classloader.ClassLoaderManager;
import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.constants.ConfigConstrant;
import com.dtstack.flinkx.options.OptionParser;
import com.dtstack.flinkx.reader.DataReader;
import com.dtstack.flinkx.reader.DataReaderFactory;
import com.dtstack.flinkx.util.ResultPrintUtil;
import com.dtstack.flinkx.writer.DataWriter;
import com.dtstack.flinkx.writer.DataWriterFactory;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.client.program.ContextEnvironment;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamContextEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.DTRebalancePartitioner;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.*;

/**
 * The main class entry
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class Main {

    public static Logger LOG = LoggerFactory.getLogger(Main.class);

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        com.dtstack.flinkx.options.Options options = new OptionParser(args).getOptions();
        String job = options.getJob();
        String jobIdString = options.getJobid();
        String monitor = options.getMonitor();
        String pluginRoot = options.getPluginRoot();
        String savepointPath = options.getS();
        Properties confProperties = parseConf(options.getConfProp());

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

        dataStream = new DataStream<>(dataStream.getExecutionEnvironment(),
                new PartitionTransformation<>(dataStream.getTransformation(),
                        new DTRebalancePartitioner<>()));

        DataWriter dataWriter = DataWriterFactory.getDataWriter(config);
        dataWriter.writeData(dataStream);

        if(env instanceof MyLocalStreamEnvironment) {
            if(StringUtils.isNotEmpty(savepointPath)){
                ((MyLocalStreamEnvironment) env).setSettings(SavepointRestoreSettings.forPath(savepointPath));
            }
        }

        Set<URL> classPathSet = ClassLoaderManager.getClassPath();
        addEnvClassPath(env, classPathSet);

        JobExecutionResult result = env.execute(jobIdString);
        if(env instanceof MyLocalStreamEnvironment){
            ResultPrintUtil.printResult(result);
        }
    }

    private static void addEnvClassPath(StreamExecutionEnvironment env, Set<URL> classPathSet) throws Exception{
        if(env instanceof MyLocalStreamEnvironment){
            ((MyLocalStreamEnvironment) env).setClasspaths(new ArrayList<>(classPathSet));
        } else if(env instanceof StreamContextEnvironment){
            Field field = env.getClass().getDeclaredField("ctx");
            field.setAccessible(true);
            ContextEnvironment contextEnvironment= (ContextEnvironment) field.get(env);

            List<String> originUrlList = new ArrayList<>();
            for (URL url : contextEnvironment.getClasspaths()) {
                originUrlList.add(url.toString());
            }

            for (URL url : classPathSet) {
                if (!originUrlList.contains(url.toString())){
                    contextEnvironment.getClasspaths().add(url);
                }
            }
        }
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
        }
        return env;
    }
}
