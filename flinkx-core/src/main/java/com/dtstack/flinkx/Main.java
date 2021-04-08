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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.types.Row;

import com.dtstack.flink.api.java.MyLocalStreamEnvironment;
import com.dtstack.flinkx.classloader.PluginUtil;
import com.dtstack.flinkx.conf.RestartConf;
import com.dtstack.flinkx.conf.SpeedConf;
import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.constants.ConfigConstant;
import com.dtstack.flinkx.enums.ClusterMode;
import com.dtstack.flinkx.options.OptionParser;
import com.dtstack.flinkx.options.Options;
import com.dtstack.flinkx.parser.SqlParser;
import com.dtstack.flinkx.sink.BaseDataSink;
import com.dtstack.flinkx.sink.DataSinkFactory;
import com.dtstack.flinkx.source.BaseDataSource;
import com.dtstack.flinkx.source.DataSourceFactory;
import com.dtstack.flinkx.util.PrintUtil;
import com.dtstack.flinkx.util.PropertiesUtil;
import com.google.common.base.Preconditions;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.dtstack.flinkx.exec.ExecuteProcessHelper.checkRemoteSqlPluginPath;
import static com.dtstack.flinkx.exec.ExecuteProcessHelper.getExternalJarUrls;
import static com.dtstack.flinkx.util.StreamEnvConfigManagerUtil.disableChainOperator;
import static com.dtstack.flinkx.util.StreamEnvConfigManagerUtil.enableUnalignedCheckpoints;
import static com.dtstack.flinkx.util.StreamEnvConfigManagerUtil.getAutoWatermarkInterval;
import static com.dtstack.flinkx.util.StreamEnvConfigManagerUtil.getBufferTimeoutMillis;
import static com.dtstack.flinkx.util.StreamEnvConfigManagerUtil.getCheckpointCleanup;
import static com.dtstack.flinkx.util.StreamEnvConfigManagerUtil.getCheckpointInterval;
import static com.dtstack.flinkx.util.StreamEnvConfigManagerUtil.getCheckpointTimeout;
import static com.dtstack.flinkx.util.StreamEnvConfigManagerUtil.getCheckpointingMode;
import static com.dtstack.flinkx.util.StreamEnvConfigManagerUtil.getDelayInterval;
import static com.dtstack.flinkx.util.StreamEnvConfigManagerUtil.getEnvParallelism;
import static com.dtstack.flinkx.util.StreamEnvConfigManagerUtil.getFailureInterval;
import static com.dtstack.flinkx.util.StreamEnvConfigManagerUtil.getMaxConcurrentCheckpoints;
import static com.dtstack.flinkx.util.StreamEnvConfigManagerUtil.getMaxEnvParallelism;
import static com.dtstack.flinkx.util.StreamEnvConfigManagerUtil.getStateBackend;
import static com.dtstack.flinkx.util.StreamEnvConfigManagerUtil.getStreamTimeCharacteristic;
import static com.dtstack.flinkx.util.StreamEnvConfigManagerUtil.isCheckpointingEnabled;
import static com.dtstack.flinkx.util.StreamEnvConfigManagerUtil.isRestore;
import static com.dtstack.flinkx.util.StreamEnvConfigManagerUtil.streamTableEnvironmentEarlyTriggerConfig;
import static com.dtstack.flinkx.util.StreamEnvConfigManagerUtil.streamTableEnvironmentName;
import static com.dtstack.flinkx.util.StreamEnvConfigManagerUtil.streamTableEnvironmentStateTTLConfig;

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
        SyncConf config = parseFlinkxConf(job, options);

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
            DataStream<Tuple2<Boolean, Row>> dataStream;
            boolean transformer = config.getTransformer() != null && StringUtils.isNotBlank(config.getTransformer().getTransformSql());
            if(transformer){
                Table sourceTable = tableEnv.fromDataStream(sourceDataStream, String.join(",", config.getReader().getFieldNameList()));
                tableEnv.registerTable(config.getReader().getTable().getTableName(), sourceTable);

                Table adaptTable = tableEnv.sqlQuery(config.getJob().getTransformer().getTransformSql());
                RowTypeInfo typeInfo = new RowTypeInfo(adaptTable.getSchema().getFieldTypes(), adaptTable.getSchema().getFieldNames());
                dataStream = tableEnv.toRetractStream(adaptTable, typeInfo);
            }else{
                dataStream = sourceDataStream.map(new MapFunction<Row, Tuple2<Boolean, Row>>() {
                            @Override
                            public Tuple2<Boolean, Row> map(Row value) {
                                return new Tuple2<>(true,value);
                            }
                        });
            }

            if (speed.isRebalance()) {
                dataStream = dataStream.rebalance();
            }

            BaseDataSink dataWriter = DataSinkFactory.getDataSink(config);
            if(transformer){
                ((StreamTableEnvironmentImpl)tableEnv).registerTableSinkInternal(config.getWriter().getTable().getTableName(), dataWriter);
            }
            DataStreamSink<?> dataStreamSink = dataWriter.writeData(dataStream);
            if(speed.getWriterChannel() > 0){
                dataStreamSink.setParallelism(speed.getWriterChannel());
            }

            JobExecutionResult result = env.execute(options.getJobName());
            if(env instanceof LocalStreamEnvironment){
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
    public static SyncConf parseFlinkxConf(String job, Options options){
        SyncConf config = null;
        try {
            config = SyncConf.parseJob(job);
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
           LOG.error(e.getMessage());
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
        StreamExecutionEnvironment env;
        if(StringUtils.equalsIgnoreCase(ClusterMode.local.name(), options.getMode())){
            env = new MyLocalStreamEnvironment(flinkConf);
        }else{
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        }
        return env;
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
    private static void configStreamExecutionEnvironment(StreamExecutionEnvironment env, Options options, SyncConf config, Properties properties) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, IOException {
        configRestartStrategy(env, config, properties);
        configCheckpoint(env, properties);
        configEnvironment(env, properties);
        if(config != null){
            PluginUtil.registerPluginUrlToCachedFile(config, env);
            env.setParallelism(config.getSpeed().getChannel());
            if(env instanceof MyLocalStreamEnvironment) {
                if(StringUtils.isNotEmpty(options.getS())){
                    ((MyLocalStreamEnvironment) env).setSettings(SavepointRestoreSettings.forPath(options.getS()));
                }
            }
        }else{
            Preconditions.checkArgument(checkRemoteSqlPluginPath(options.getRemotePluginPath(), options.getMode(), options.getPluginLoadMode()),
                    "Non-local mode or shipfile deployment mode, remoteSqlPluginPath is required");
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
        streamTableEnvironmentStateTTLConfig(tableEnv, properties);
        streamTableEnvironmentEarlyTriggerConfig(tableEnv, properties);
        streamTableEnvironmentName(tableEnv, jobName);
    }


        /**
         * flink任务设置重试策略
         * @param env
         * @param config
         * @param properties
         */
    private static void configRestartStrategy(StreamExecutionEnvironment env, SyncConf config, Properties properties){
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
            if (isRestore(properties).get()) {
                env.setRestartStrategy(RestartStrategies.failureRateRestart(
                        ConfigConstant.FAILUEE_RATE,
                        Time.of(getFailureInterval(properties).get(), TimeUnit.MINUTES),
                        Time.of(getDelayInterval(properties).get(), TimeUnit.SECONDS)
                ));
            } else {
                env.setRestartStrategy(RestartStrategies.noRestart());
            }
        }
    }

    /**
     * 配置最大并行度等相关参数
     * @param streamEnv
     * @param confProperties
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     */
    private static void configEnvironment(StreamExecutionEnvironment streamEnv, Properties confProperties) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        confProperties = PropertiesUtil.propertiesTrim(confProperties);
        streamEnv.getConfig().disableClosureCleaner();

        Configuration globalJobParameters = new Configuration();
        //Configuration unsupported set properties key-value
        Method method = Configuration.class.getDeclaredMethod("setValueInternal", String.class, Object.class);
        method.setAccessible(true);
        for (Map.Entry<Object, Object> prop : confProperties.entrySet()) {
            method.invoke(globalJobParameters, prop.getKey(), prop.getValue());
        }

        ExecutionConfig exeConfig = streamEnv.getConfig();
        if (exeConfig.getGlobalJobParameters() == null) {
            exeConfig.setGlobalJobParameters(globalJobParameters);
        } else if (exeConfig.getGlobalJobParameters() != null) {
            exeConfig.setGlobalJobParameters(globalJobParameters);
        }

        disableChainOperator(streamEnv, globalJobParameters);

        getEnvParallelism(confProperties).ifPresent(streamEnv::setParallelism);
        getMaxEnvParallelism(confProperties).ifPresent(streamEnv::setMaxParallelism);
        getBufferTimeoutMillis(confProperties).ifPresent(streamEnv::setBufferTimeout);
        getStreamTimeCharacteristic(confProperties).ifPresent(streamEnv::setStreamTimeCharacteristic);
        getAutoWatermarkInterval(confProperties).ifPresent(op -> {
            if (streamEnv.getStreamTimeCharacteristic().equals(TimeCharacteristic.EventTime)) {
                streamEnv.getConfig().setAutoWatermarkInterval(op);
            }
        });
    }

    /**
     * flink任务设置checkpoint
     * @param env
     * @param properties
     * @return
     */
    private static void configCheckpoint(StreamExecutionEnvironment env, Properties properties) throws IOException {
        Optional<Boolean> checkpointingEnabled = isCheckpointingEnabled(properties);
        if (checkpointingEnabled.get()) {
            getCheckpointInterval(properties).ifPresent(env::enableCheckpointing);
            getCheckpointingMode(properties).ifPresent(env.getCheckpointConfig()::setCheckpointingMode);
            getCheckpointTimeout(properties).ifPresent(env.getCheckpointConfig()::setCheckpointTimeout);
            getMaxConcurrentCheckpoints(properties).ifPresent(env.getCheckpointConfig()::setMaxConcurrentCheckpoints);
            getCheckpointCleanup(properties).ifPresent(env.getCheckpointConfig()::enableExternalizedCheckpoints);
            enableUnalignedCheckpoints(properties).ifPresent(event -> env.getCheckpointConfig().enableUnalignedCheckpoints(event));
            getStateBackend(properties).ifPresent(env::setStateBackend);
        }
    }
}
