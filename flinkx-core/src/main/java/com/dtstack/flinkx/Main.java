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
import com.dtstack.flinkx.conf.FlinkxConf;
import com.dtstack.flinkx.conf.RestartConf;
import com.dtstack.flinkx.conf.RestoreConf;
import com.dtstack.flinkx.conf.SpeedConf;
import com.dtstack.flinkx.constants.ConfigConstant;
import com.dtstack.flinkx.options.OptionParser;
import com.dtstack.flinkx.source.BaseDataSource;
import com.dtstack.flinkx.source.DataSourceFactory;
import com.dtstack.flinkx.util.PrintUtil;
import com.dtstack.flinkx.util.PropertiesUtil;
import com.dtstack.flinkx.sink.BaseDataSink;
import com.dtstack.flinkx.sink.DataWriterFactory;
import org.apache.commons.lang.StringUtils;
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

    public static void main(String[] args) throws Exception {
        com.dtstack.flinkx.options.Options options = new OptionParser(args).getOptions();
        String job = options.getJob();
        String jobIdString = options.getJobName();
        String monitor = options.getMonitor();
        String pluginRoot = options.getPluginRoot();
        String savepointPath = options.getS();
        String remotePluginPath = options.getRemotePluginPath();
        Properties confProperties = PropertiesUtil.parseConf(options.getConfProp());

        // 解析jobPath指定的任务配置文件
        FlinkxConf flinkxConf = FlinkxConf.parseJob(job);

        if(org.apache.commons.lang3.StringUtils.isNotEmpty(monitor)) {
            flinkxConf.setMonitorUrls(monitor);
        }

        if(org.apache.commons.lang3.StringUtils.isNotEmpty(pluginRoot)) {
            flinkxConf.setPluginRoot(pluginRoot);
        }

        if (org.apache.commons.lang3.StringUtils.isNotEmpty(remotePluginPath)) {
            flinkxConf.setRemotePluginPath(remotePluginPath);
        }

        Configuration flinkConf = new Configuration();
        if (org.apache.commons.lang3.StringUtils.isNotEmpty(options.getFlinkconf())) {
            flinkConf = GlobalConfiguration.loadConfiguration(options.getFlinkconf());
        }

        StreamExecutionEnvironment env = (org.apache.commons.lang3.StringUtils.isNotBlank(monitor)) ?
                StreamExecutionEnvironment.getExecutionEnvironment() :
                new MyLocalStreamEnvironment(flinkConf);

        env = openCheckpointConf(env, confProperties);
        configRestartStrategy(env, flinkxConf);
        PluginUtil.registerPluginUrlToCachedFile(flinkxConf, env);

        SpeedConf speed = flinkxConf.getSpeed();

        env.setParallelism(speed.getChannel());
        BaseDataSource dataReader = DataSourceFactory.getDataSource(flinkxConf, env);
        DataStream<Row> dataStream = dataReader.readData();
        if(speed.getReaderChannel() > 0){
            dataStream = ((DataStreamSource<Row>) dataStream).setParallelism(speed.getReaderChannel());
        }

        if (speed.isRebalance()) {
            dataStream = dataStream.rebalance();
        }

        BaseDataSink dataWriter = DataWriterFactory.getDataWriter(flinkxConf);
        DataStreamSink<?> dataStreamSink = dataWriter.writeData(dataStream);
        if(speed.getWriterChannel() > 0){
            dataStreamSink.setParallelism(speed.getWriterChannel());
        }

        if(env instanceof MyLocalStreamEnvironment) {
            if(org.apache.commons.lang3.StringUtils.isNotEmpty(savepointPath)){
                ((MyLocalStreamEnvironment) env).setSettings(SavepointRestoreSettings.forPath(savepointPath));
            }
        }

        JobExecutionResult result = env.execute(jobIdString);
        if(env instanceof MyLocalStreamEnvironment){
            PrintUtil.printResult(result);
        }
    }

    private static void configRestartStrategy(StreamExecutionEnvironment env, FlinkxConf config){
        RestoreConf restore = config.getRestore();
        if (restore.isStream()) {
            RestartConf restart = config.getRestart();

            if (ConfigConstant.STRATEGY_FIXED_DELAY.equalsIgnoreCase(restart.getStrategy())) {
                env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                        restart.getRestartAttempts(),
                        Time.of(restart.getDelayInterval(), TimeUnit.SECONDS)
                ));
            } else if (ConfigConstant.STRATEGY_FAILURE_RATE.equalsIgnoreCase(restart.getStrategy())) {
                env.setRestartStrategy(RestartStrategies.failureRateRestart(
                        restart.getFailureRate(),
                        Time.of(restart.getFailureInterval(), TimeUnit.SECONDS),
                        Time.of(restart.getDelayInterval(), TimeUnit.SECONDS)
                ));
            } else {
                env.setRestartStrategy(RestartStrategies.noRestart());
            }
        } else {
            env.setRestartStrategy(RestartStrategies.noRestart());
        }
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
