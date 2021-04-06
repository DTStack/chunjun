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
import com.dtstack.flinkx.conf.SpeedConf;
import com.dtstack.flinkx.constants.ConfigConstant;
import com.dtstack.flinkx.environment.StreamEnvConfigManager;
import com.dtstack.flinkx.options.OptionParser;
import com.dtstack.flinkx.options.Options;
import com.dtstack.flinkx.parser.SqlParser;
import com.dtstack.flinkx.sink.BaseDataSink;
import com.dtstack.flinkx.sink.DataWriterFactory;
import com.dtstack.flinkx.source.BaseDataSource;
import com.dtstack.flinkx.source.DataSourceFactory;
import com.dtstack.flinkx.util.PrintUtil;
import com.dtstack.flinkx.util.PropertiesUtil;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.dtstack.flinkx.exec.ExecuteProcessHelper.getExternalJarUrls;

/**
 * The main class entry
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class Main {

    public static Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        LOG.info("------------program params-------------------------");
        Arrays.stream(args).forEach(arg -> LOG.info("{}", arg));
        LOG.info("-------------------------------------------");

        Options options = new OptionParser(args).getOptions();
        String job = URLDecoder.decode(options.getJob(), Charsets.UTF_8.name());
        Properties confProperties = PropertiesUtil.parseConf(options.getConfProp());

        // 解析jobPath指定的任务配置文件
        FlinkxConf config = parseFlinkxConf(job, options);

        StreamExecutionEnvironment env = createStreamExecutionEnvironment(options);
        configStreamExecutionEnvironment(env, options, config, confProperties);

        StreamTableEnvironment tableEnv = createStreamTableEnvironment(env);
        configStreamExecutionEnvironment(tableEnv, confProperties, options.getJobName());

        if(config != null){
            SpeedConf speed = config.getSpeed();
            BaseDataSource dataReader = DataSourceFactory.getDataSource(config, env);
            DataStream<Row> sourceDataStream = dataReader.readData();
            if(speed.getReaderChannel() > 0){
                sourceDataStream = ((DataStreamSource<Row>) sourceDataStream).setParallelism(speed.getReaderChannel());
            }

            Table sourceTable = tableEnv.fromDataStream(sourceDataStream, String.join(",", config.getReader().getFieldNameList()));
            tableEnv.registerTable(config.getReader().getTable().getTableName(), sourceTable);

            Table adaptTable = tableEnv.sqlQuery(config.getJob().getTransformer().getTransformSql());
            RowTypeInfo typeInfo = new RowTypeInfo(adaptTable.getSchema().getFieldTypes(), adaptTable.getSchema().getFieldNames());
            DataStream<Tuple2<Boolean, Row>> dataStream = tableEnv.toRetractStream(adaptTable, typeInfo);

            if (speed.isRebalance()) {
                dataStream = dataStream.rebalance();
            }

            BaseDataSink dataWriter = DataWriterFactory.getDataWriter(config);
            ((StreamTableEnvironmentImpl)tableEnv).registerTableSinkInternal(config.getWriter().getTable().getTableName(), dataWriter);
            DataStreamSink<?> dataStreamSink = dataWriter.writeData(dataStream);
            if(speed.getWriterChannel() > 0){
                dataStreamSink.setParallelism(speed.getWriterChannel());
            }

            JobExecutionResult result = env.execute(options.getJobName());
            if(env instanceof MyLocalStreamEnvironment){
                PrintUtil.printResult(result);
            }
        }else{
            List<URL> jarUrlList = getExternalJarUrls(options.getAddjar());
            StatementSet statementSet = SqlParser.parseSql(job, jarUrlList, tableEnv);
            statementSet.execute();
        }
        LOG.info("program {} execution success", options.getJobName());
    }

    /**
     * 解析并设置job
     * @param job
     * @param options
     * @return
     */
    public static FlinkxConf parseFlinkxConf(String job, Options options){
        FlinkxConf config = null;
        try {
            config = FlinkxConf.parseJob(job);
            if(StringUtils.isNotEmpty(options.getMonitor())) {
                config.setMonitorUrls(options.getMonitor());
            }

            if(StringUtils.isNotEmpty(options.getPluginRoot())) {
                config.setPluginRoot(options.getPluginRoot());
            }

            if (StringUtils.isNotEmpty(options.getRemotePluginPath())) {
                config.setRemotePluginPath(options.getRemotePluginPath());
            }
        }catch (Exception e){
            //todo 简易判断是否为SQL任务
        }
        return config;
    }

    /**
     * 创建StreamExecutionEnvironment
     * @param options
     * @return
     */
    private static StreamExecutionEnvironment createStreamExecutionEnvironment(Options options){
        Configuration flinkConf = new Configuration();
        if (StringUtils.isNotEmpty(options.getFlinkconf())) {
            flinkConf = GlobalConfiguration.loadConfiguration(options.getFlinkconf());
        }
        return StreamExecutionEnvironment.getExecutionEnvironment(flinkConf);
    }

    /**
     * 创建StreamTableEnvironment
     * @param env StreamExecutionEnvironment
     * @return
     */
    private static StreamTableEnvironment createStreamTableEnvironment(StreamExecutionEnvironment env){
        // use blink and stream mode
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        TableConfig tableConfig = new TableConfig();
        return StreamTableEnvironmentImpl.create(env, settings, tableConfig);
    }

    /**
     * 配置StreamExecutionEnvironment
     * @param env StreamExecutionEnvironment
     * @param options options
     * @param config FlinkxConf
     * @param properties confProperties
     */
    private static void configStreamExecutionEnvironment(StreamExecutionEnvironment env, Options options, FlinkxConf config, Properties properties){
        configRestartStrategy(env, config, properties);
        configCheckpoint(env, properties);
        if(config != null){
            PluginUtil.registerPluginUrlToCachedFile(config, env);
            env.setParallelism(config.getSpeed().getChannel());
            if(env instanceof MyLocalStreamEnvironment) {
                if(StringUtils.isNotEmpty(options.getS())){
                    ((MyLocalStreamEnvironment) env).setSettings(SavepointRestoreSettings.forPath(options.getS()));
                }
            }
        }else{
            FactoryUtil.setPluginPath(StringUtils.isNotEmpty(options.getPluginRoot()) ? options.getPluginRoot() : options.getRemotePluginPath());
            FactoryUtil.setEnv(env);
            FactoryUtil.setConnectorLoadMode(options.getConnectorLoadMode());
        }
    }

    /**
     * 配置StreamTableEnvironment
     * @param tableEnv
     * @param properties
     * @param jobName
     */
    private static void configStreamExecutionEnvironment(StreamTableEnvironment tableEnv, Properties properties, String jobName){
        //todo StreamEnvConfigManager方法迁移
        StreamEnvConfigManager.streamTableEnvironmentStateTTLConfig(tableEnv, properties);
        StreamEnvConfigManager.streamTableEnvironmentEarlyTriggerConfig(tableEnv, properties);
        StreamEnvConfigManager.streamTableEnvironmentName(tableEnv, jobName);
    }


        /**
         * flink任务设置重试策略
         * @param env
         * @param config
         * @param properties
         */
    private static void configRestartStrategy(StreamExecutionEnvironment env, FlinkxConf config, Properties properties){
        if(config != null){
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
        }else{
            //todo SQL任务设置重试策略

        }
    }

    /**
     * flink任务设置checkpoint
     * @param env
     * @param properties
     * @return
     */
    private static void configCheckpoint(StreamExecutionEnvironment env, Properties properties){
        if(properties != null){
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
            env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        }
    }
}
