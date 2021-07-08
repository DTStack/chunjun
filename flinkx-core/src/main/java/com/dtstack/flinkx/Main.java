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
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionParser;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import com.dtstack.flinkx.conf.RestartConf;
import com.dtstack.flinkx.conf.SpeedConf;
import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.constants.ConfigConstant;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.enums.ClusterMode;
import com.dtstack.flinkx.enums.EJobType;
import com.dtstack.flinkx.environment.MyLocalStreamEnvironment;
import com.dtstack.flinkx.exec.ExecuteProcessHelper;
import com.dtstack.flinkx.options.OptionParser;
import com.dtstack.flinkx.options.Options;
import com.dtstack.flinkx.sink.SinkFactory;
import com.dtstack.flinkx.source.SourceFactory;
import com.dtstack.flinkx.sql.parser.SqlParser;
import com.dtstack.flinkx.throwable.FlinkxRuntimeException;
import com.dtstack.flinkx.util.DataSyncFactoryUtil;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.MathUtil;
import com.dtstack.flinkx.util.PluginUtil;
import com.dtstack.flinkx.util.PrintUtil;
import com.dtstack.flinkx.util.PropertiesUtil;
import com.dtstack.flinkx.util.StreamEnvConfigManagerUtil;
import com.dtstack.flinkx.util.TableUtil;
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


/**
 * The main class entry
 *
 * <p>Company: www.dtstack.com
 *
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

        StreamExecutionEnvironment env = createStreamExecutionEnvironment(options);
        StreamTableEnvironment tableEnv = createStreamTableEnvironment(env);
        configStreamExecutionEnvironment(tableEnv, confProperties, options.getJobName());

        switch (EJobType.getByName(options.getJobType())) {
            case SQL:
                exeSqlJob(env, tableEnv, job, options, confProperties);
                break;

            case SYNC:
                exeSyncJob(env, tableEnv, job, options, confProperties);
                break;
            default:
                throw new FlinkxRuntimeException("unknown jobType: [" + options.getJobType() + "], jobType must in [SQL, SYNC].");
        }

        LOG.info("program {} execution success", options.getJobName());
    }

    /**
     * 执行sql 类型任务
     *
     * @param env
     * @param tableEnv
     * @param job
     * @param options
     * @param confProperties
     * @throws Exception
     */
    private static void exeSqlJob(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String job, Options options, Properties confProperties) throws Exception {
        configStreamExecutionEnvironment(env, options, null, confProperties);
        List<URL> jarUrlList = ExecuteProcessHelper.getExternalJarUrls(options.getAddjar());
        StatementSet statementSet = SqlParser.parseSql(job, jarUrlList, tableEnv);
        TableResult execute = statementSet.execute();
        if (env instanceof MyLocalStreamEnvironment) {
            execute.getJobClient().ifPresent(v -> {
                try {
                    PrintUtil.printResult(v.getAccumulators().get());
                } catch (Exception e) {
                    LOG.error("error to execute sql job, e = {}", ExceptionUtil.getErrorMessage(e));
                }
            });
        }
    }

    /**
     * 执行 数据同步类型任务
     *
     * @param env
     * @param tableEnv
     * @param job
     * @param options
     * @param confProperties
     * @throws Exception
     */
    private static void exeSyncJob(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String job, Options options, Properties confProperties) throws Exception {
        SyncConf config = parseFlinkxConf(job, options);
        buildRestartStrategy(confProperties, config);
        configStreamExecutionEnvironment(env, options, config, confProperties);

        SourceFactory sourceFactory = DataSyncFactoryUtil.discoverSource(config, env);
        DataStream<RowData> dataStreamSource = sourceFactory.createSource();

        SpeedConf speed = config.getSpeed();
        if (speed.getReaderChannel() > 0) {
            dataStreamSource = ((DataStreamSource<RowData>) dataStreamSource).setParallelism(speed.getReaderChannel());
        }

        DataStream<RowData> dataStream;
        boolean transformer = config.getTransformer() != null && StringUtils.isNotBlank(config.getTransformer().getTransformSql());

        if (transformer) {
            dataStream = syncStreamToTable(tableEnv, config, dataStreamSource);
        } else {
            dataStream = dataStreamSource;
        }

        if (speed.isRebalance()) {
            dataStream = dataStream.rebalance();
        }

        SinkFactory sinkFactory = DataSyncFactoryUtil.discoverSink(config);
        DataStreamSink<RowData> dataStreamSink = sinkFactory.createSink(dataStream);
        if (speed.getWriterChannel() > 0) {
            dataStreamSink.setParallelism(speed.getWriterChannel());
        }

        JobExecutionResult result = env.execute(options.getJobName());
        if (env instanceof MyLocalStreamEnvironment) {
            PrintUtil.printResult(result.getAllAccumulatorResults());
        }
    }

    /**
     * 将数据同步Stream 注册成table
     *
     * @param tableEnv
     * @param config
     * @param sourceDataStream
     * @return
     */
    private static DataStream<RowData> syncStreamToTable( StreamTableEnvironment tableEnv, SyncConf config, DataStream<RowData> sourceDataStream) {
        String fieldNames = String.join(ConstantValue.COMMA_SYMBOL, config.getReader().getFieldNameList());
        List<Expression> expressionList = ExpressionParser.parseExpressionList(fieldNames);
        Table sourceTable = tableEnv.fromDataStream(sourceDataStream, expressionList.toArray(new Expression[0]));
        tableEnv.createTemporaryView(config.getReader().getTable().getTableName(), sourceTable);

        String transformSql = config.getJob().getTransformer().getTransformSql();
        Table adaptTable = tableEnv.sqlQuery(transformSql);

        DataType[] tableDataTypes = adaptTable.getSchema().getFieldDataTypes();
        String[] tableFieldNames = adaptTable.getSchema().getFieldNames();
        TypeInformation<RowData> typeInformation = TableUtil.getTypeInformation(tableDataTypes, tableFieldNames);
        DataStream<RowData> dataStream = tableEnv.toRetractStream(adaptTable, typeInformation).map(f -> f.f1);
        tableEnv.createTemporaryView(config.getWriter().getTable().getTableName(), dataStream);

        return dataStream;
    }

    /**
     * 将配置文件中的任务重启策略设置到Properties中
     *
     * @param confProperties
     * @param config
     */
    private static void buildRestartStrategy(Properties confProperties, SyncConf config) {
        RestartConf restart = config.getRestart();
        confProperties.setProperty(ConfigConstant.STRATEGY_STRATEGY, restart.getStrategy());
        confProperties.setProperty(ConfigConstant.STRATEGY_RESTARTATTEMPTS, String.valueOf(restart.getRestartAttempts()));
        confProperties.setProperty(ConfigConstant.STRATEGY_DELAYINTERVAL, String.valueOf(restart.getDelayInterval()));
        confProperties.setProperty(ConfigConstant.STRATEGY_FAILURERATE, String.valueOf(restart.getFailureRate()));
        confProperties.setProperty(ConfigConstant.STRATEGY_FAILUREINTERVAL, String.valueOf(restart.getFailureInterval()));
    }

    /**
     * 解析并设置job
     *
     * @param job
     * @param options
     * @return
     */
    public static SyncConf parseFlinkxConf(String job, Options options) {
        SyncConf config;
        try {
            config = SyncConf.parseJob(job);

            if (StringUtils.isNotBlank(options.getPluginRoot())) {
                config.setPluginRoot(options.getPluginRoot());
            }

            if (StringUtils.isNotBlank(options.getRemotePluginPath())) {
                config.setRemotePluginPath(options.getRemotePluginPath());
            }

            if(StringUtils.isNotBlank(options.getS())){
                config.setRestorePath(options.getS());
            }
        } catch (Exception e) {
            throw new FlinkxRuntimeException(e);
        }
        return config;
    }

    /**
     * 创建StreamExecutionEnvironment
     *
     * @param options
     * @return
     */
    private static StreamExecutionEnvironment createStreamExecutionEnvironment(Options options) {
        Configuration flinkConf = new Configuration();
        if (StringUtils.isNotEmpty(options.getFlinkconf())) {
            flinkConf = GlobalConfiguration.loadConfiguration(options.getFlinkconf());
        }
        StreamExecutionEnvironment env;
        if (StringUtils.equalsIgnoreCase(ClusterMode.local.name(), options.getMode())) {
            env = new MyLocalStreamEnvironment(flinkConf);
        } else {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        }
        return env;
    }

    /**
     * 创建StreamTableEnvironment
     *
     * @param env StreamExecutionEnvironment
     * @return
     */
    private static StreamTableEnvironment createStreamTableEnvironment(StreamExecutionEnvironment env) {
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Configuration configuration = tEnv.getConfig().getConfiguration();
        // Iceberg need this config setting up true.
        configuration.setBoolean(TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED.key(), true);
        return tEnv;
    }

    /**
     * 配置StreamExecutionEnvironment
     *
     * @param env StreamExecutionEnvironment
     * @param options options
     * @param config FlinkxConf
     * @param properties confProperties
     */
    private static void configStreamExecutionEnvironment(StreamExecutionEnvironment env, Options options, SyncConf config, Properties properties) throws Exception {
        configRestartStrategy(env, properties);
        configCheckpoint(env, properties);
        configEnvironment(env, properties);
        if (env instanceof MyLocalStreamEnvironment) {
            if (StringUtils.isNotEmpty(options.getS())) {
                ((MyLocalStreamEnvironment) env).setSettings(SavepointRestoreSettings.forPath(options.getS()));
            }
        }

        if (config != null) {
            PluginUtil.registerPluginUrlToCachedFile(config, env);
            env.setParallelism(config.getSpeed().getChannel());
        } else {
            Preconditions.checkArgument(ExecuteProcessHelper.checkRemoteSqlPluginPath( options.getRemotePluginPath(), options.getMode(), options.getPluginLoadMode()), "Non-local mode or shipfile deployment mode, remoteSqlPluginPath is required");
            FactoryUtil.setLocalPluginPath(options.getPluginRoot());
            FactoryUtil.setRemotePluginPath(options.getRemotePluginPath());
            FactoryUtil.setPluginLoadMode(options.getPluginLoadMode());
            FactoryUtil.setEnv(env);
            FactoryUtil.setConnectorLoadMode(options.getConnectorLoadMode());
        }
    }

    /**
     * 配置StreamTableEnvironment
     *
     * @param tableEnv
     * @param properties
     * @param jobName
     */
    private static void configStreamExecutionEnvironment(StreamTableEnvironment tableEnv, Properties properties, String jobName) {
        StreamEnvConfigManagerUtil.streamTableEnvironmentStateTTLConfig(tableEnv, properties);
        StreamEnvConfigManagerUtil.streamTableEnvironmentEarlyTriggerConfig(tableEnv, properties);
        StreamEnvConfigManagerUtil.streamTableEnvironmentName(tableEnv, jobName);
    }

    /**
     * flink任务设置重试策略
     *
     * @param env
     * @param properties
     */
    private static void configRestartStrategy(StreamExecutionEnvironment env, Properties properties) {
        String strategy = properties.getProperty(ConfigConstant.STRATEGY_STRATEGY);
        if (StringUtils.equalsIgnoreCase(ConfigConstant.STRATEGY_FIXED_DELAY, strategy)) {
            Integer restartAttempts = MathUtil.getIntegerVal(properties.getProperty(ConfigConstant.STRATEGY_RESTARTATTEMPTS));
            Long delayInterval = MathUtil.getLongVal(properties.getProperty(ConfigConstant.STRATEGY_DELAYINTERVAL));
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(restartAttempts, Time.of(delayInterval, TimeUnit.SECONDS)));
        } else if (StringUtils.equalsIgnoreCase(ConfigConstant.STRATEGY_FAILURE_RATE, strategy) || StreamEnvConfigManagerUtil.isRestore(properties).get()) {
            Integer failureRate = MathUtil.getIntegerVal(properties.getProperty(ConfigConstant.STRATEGY_FAILURERATE), ConfigConstant.FAILUEE_RATE);
            Long failureInterval = MathUtil.getLongVal(properties.getProperty(ConfigConstant.STRATEGY_FAILUREINTERVAL), StreamEnvConfigManagerUtil.getFailureInterval(properties).get());
            Long delayInterval = MathUtil.getLongVal(properties.getProperty(ConfigConstant.STRATEGY_DELAYINTERVAL), StreamEnvConfigManagerUtil.getDelayInterval(properties).get());
            env.setRestartStrategy(RestartStrategies.failureRateRestart(failureRate, Time.of(failureInterval, TimeUnit.SECONDS), Time.of(delayInterval, TimeUnit.SECONDS)));
        } else {
            env.setRestartStrategy(RestartStrategies.noRestart());
        }
    }

    /**
     * 配置最大并行度等相关参数
     *
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
        // Configuration unsupported set properties key-value
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

        StreamEnvConfigManagerUtil.disableChainOperator(streamEnv, globalJobParameters);

        StreamEnvConfigManagerUtil.getEnvParallelism(confProperties).ifPresent(streamEnv::setParallelism);
        StreamEnvConfigManagerUtil.getMaxEnvParallelism(confProperties).ifPresent(streamEnv::setMaxParallelism);
        StreamEnvConfigManagerUtil.getBufferTimeoutMillis(confProperties).ifPresent(streamEnv::setBufferTimeout);
        StreamEnvConfigManagerUtil.getStreamTimeCharacteristic(confProperties).ifPresent(streamEnv::setStreamTimeCharacteristic);
        StreamEnvConfigManagerUtil.getAutoWatermarkInterval(confProperties)
                .ifPresent(
                        op -> {
                            if (streamEnv
                                    .getStreamTimeCharacteristic()
                                    .equals(TimeCharacteristic.EventTime)) {
                                streamEnv.getConfig().setAutoWatermarkInterval(op);
                            }
                        });
    }

    /**
     * flink任务设置checkpoint
     *
     * @param env
     * @param properties
     * @return
     */
    private static void configCheckpoint(StreamExecutionEnvironment env, Properties properties) throws IOException {
        Optional<Boolean> checkpointEnabled = StreamEnvConfigManagerUtil.isCheckpointEnabled(properties);
        if (checkpointEnabled.get()) {
            StreamEnvConfigManagerUtil.getTolerableCheckpointFailureNumber(properties).ifPresent(env.getCheckpointConfig()::setTolerableCheckpointFailureNumber);
            StreamEnvConfigManagerUtil.getCheckpointInterval(properties).ifPresent(env::enableCheckpointing);
            StreamEnvConfigManagerUtil.getCheckpointMode(properties).ifPresent(env.getCheckpointConfig()::setCheckpointingMode);
            StreamEnvConfigManagerUtil.getCheckpointTimeout(properties).ifPresent(env.getCheckpointConfig()::setCheckpointTimeout);
            StreamEnvConfigManagerUtil.getMaxConcurrentCheckpoints(properties).ifPresent(env.getCheckpointConfig()::setMaxConcurrentCheckpoints);
            StreamEnvConfigManagerUtil.getCheckpointCleanup(properties).ifPresent(env.getCheckpointConfig()::enableExternalizedCheckpoints);
            StreamEnvConfigManagerUtil.enableUnalignedCheckpoints(properties).ifPresent(event -> env.getCheckpointConfig().enableUnalignedCheckpoints(event));
            StreamEnvConfigManagerUtil.getStateBackend(properties).ifPresent(env::setStateBackend);
        }
    }
}
