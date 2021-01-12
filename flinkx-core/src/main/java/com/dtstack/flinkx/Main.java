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
import com.dtstack.flinkx.classloader.PluginUtil;
import com.dtstack.flinkx.config.ContentConfig;
import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.RestartConfig;
import com.dtstack.flinkx.config.SpeedConfig;
import com.dtstack.flinkx.config.TestConfig;
import com.dtstack.flinkx.constants.ConfigConstant;
import com.dtstack.flinkx.options.OptionParser;
import com.dtstack.flinkx.reader.BaseDataReader;
import com.dtstack.flinkx.reader.DataReaderFactory;
import com.dtstack.flinkx.util.ResultPrintUtil;
import com.dtstack.flinkx.writer.BaseDataWriter;
import com.dtstack.flinkx.writer.DataWriterFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLDecoder;
import java.util.Map;
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

    public static final String READER = "reader";
    public static final String WRITER = "writer";
    public static final String STREAM_READER = "streamreader";
    public static final String STREAM_WRITER = "streamwriter";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        com.dtstack.flinkx.options.Options options = new OptionParser(args).getOptions();
        String job = options.getJob();
        String jobIdString = options.getJobid();
        String monitor = options.getMonitor();
        String pluginRoot = options.getPluginRoot();
        String savepointPath = options.getS();
        String remotePluginPath = options.getRemotePluginPath();
        Properties confProperties = parseConf(options.getConfProp());

        // 解析jobPath指定的任务配置文件
        DataTransferConfig config = DataTransferConfig.parse(job);
        speedTest(config);

        if(StringUtils.isNotEmpty(monitor)) {
            config.setMonitorUrls(monitor);
        }

        if(StringUtils.isNotEmpty(pluginRoot)) {
            config.setPluginRoot(pluginRoot);
        }

        if (StringUtils.isNotEmpty(remotePluginPath)) {
            config.setRemotePluginPath(remotePluginPath);
        }

        Configuration flinkConf = new Configuration();
        if (StringUtils.isNotEmpty(options.getFlinkconf())) {
            flinkConf = GlobalConfiguration.loadConfiguration(options.getFlinkconf());
        }

        StreamExecutionEnvironment env = (StringUtils.isNotBlank(monitor)) ?
                StreamExecutionEnvironment.getExecutionEnvironment() :
                new MyLocalStreamEnvironment(flinkConf);

        env = openCheckpointConf(env, confProperties);
        configRestartStrategy(env, config);

        SpeedConfig speedConfig = config.getJob().getSetting().getSpeed();

        PluginUtil.registerPluginUrlToCachedFile(config, env);

        env.setParallelism(speedConfig.getChannel());
        env.setRestartStrategy(RestartStrategies.noRestart());
        BaseDataReader dataReader = DataReaderFactory.getDataReader(config, env);
        DataStream<Row> dataStream = dataReader.readData();
        if(speedConfig.getReaderChannel() > 0){
            dataStream = ((DataStreamSource<Row>) dataStream).setParallelism(speedConfig.getReaderChannel());
        }

        if (speedConfig.isRebalance()) {
            dataStream = dataStream.rebalance();
        }

        BaseDataWriter dataWriter = DataWriterFactory.getDataWriter(config);
        DataStreamSink<?> dataStreamSink = dataWriter.writeData(dataStream);
        if(speedConfig.getWriterChannel() > 0){
            dataStreamSink.setParallelism(speedConfig.getWriterChannel());
        }

        if(env instanceof MyLocalStreamEnvironment) {
            if(StringUtils.isNotEmpty(savepointPath)){
                ((MyLocalStreamEnvironment) env).setSettings(SavepointRestoreSettings.forPath(savepointPath));
            }
        }

        JobExecutionResult result = env.execute(jobIdString);
        if(env instanceof MyLocalStreamEnvironment){
            ResultPrintUtil.printResult(result);
        }
    }

    private static void configRestartStrategy(StreamExecutionEnvironment env, DataTransferConfig config){
        if (needRestart(config)) {
            RestartConfig restartConfig = findRestartConfig(config);
            if (RestartConfig.STRATEGY_FIXED_DELAY.equalsIgnoreCase(restartConfig.getStrategy())) {
                env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                        restartConfig.getRestartAttempts(),
                        Time.of(restartConfig.getDelayInterval(), TimeUnit.SECONDS)
                ));
            } else if (RestartConfig.STRATEGY_FAILURE_RATE.equalsIgnoreCase(restartConfig.getStrategy())) {
                env.setRestartStrategy(RestartStrategies.failureRateRestart(
                        restartConfig.getFailureRate(),
                        Time.of(restartConfig.getFailureInterval(), TimeUnit.SECONDS),
                        Time.of(restartConfig.getDelayInterval(), TimeUnit.SECONDS)
                ));
            } else {
                env.setRestartStrategy(RestartStrategies.noRestart());
            }
        } else {
            env.setRestartStrategy(RestartStrategies.noRestart());
        }
    }

    private static RestartConfig findRestartConfig(DataTransferConfig config) {
        RestartConfig restartConfig = config.getJob().getSetting().getRestartConfig();
        if (null != restartConfig) {
            return restartConfig;
        }

        Object restartConfigObj = config.getJob().getContent().get(0).getReader().getParameter().getVal(RestartConfig.KEY_STRATEGY);
        if (null != restartConfigObj) {
            return new RestartConfig((Map<String, Object>)restartConfigObj);
        }

        restartConfigObj = config.getJob().getContent().get(0).getWriter().getParameter().getVal(RestartConfig.KEY_STRATEGY);
        if (null != restartConfigObj) {
            return new RestartConfig((Map<String, Object>)restartConfigObj);
        }

        return RestartConfig.defaultConfig();
    }

    private static boolean needRestart(DataTransferConfig config){
        return config.getJob().getSetting().getRestoreConfig().isStream();
    }

    private static void speedTest(DataTransferConfig config) {
        TestConfig testConfig = config.getJob().getSetting().getTestConfig();
        if (READER.equalsIgnoreCase(testConfig.getSpeedTest())) {
            ContentConfig contentConfig = config.getJob().getContent().get(0);
            contentConfig.getWriter().setName(STREAM_WRITER);
        } else if (WRITER.equalsIgnoreCase(testConfig.getSpeedTest())){
            ContentConfig contentConfig = config.getJob().getContent().get(0);
            contentConfig.getReader().setName(STREAM_READER);
        }else {
            return;
        }

        config.getJob().getSetting().getSpeed().setBytes(-1);
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
            String interval = properties.getProperty(ConfigConstant.FLINK_CHECKPOINT_INTERVAL_KEY);
            if(StringUtils.isNotBlank(interval)){
                env.enableCheckpointing(Long.parseLong(interval.trim()));
                LOG.info("Open checkpoint with interval:" + interval);
            }
            String checkpointTimeoutStr = properties.getProperty(ConfigConstant.FLINK_CHECKPOINT_TIMEOUT_KEY);
            if(checkpointTimeoutStr != null){
                long checkpointTimeout = Long.parseLong(checkpointTimeoutStr.trim());
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
