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
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionParser;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.LogicalType;

import com.dtstack.flinkx.conf.RestartConf;
import com.dtstack.flinkx.conf.SpeedConf;
import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.constants.ConfigConstant;
import com.dtstack.flinkx.enums.ClusterMode;
import com.dtstack.flinkx.enums.EJobType;
import com.dtstack.flinkx.environment.MyLocalStreamEnvironment;
import com.dtstack.flinkx.exec.ExecuteProcessHelper;
import com.dtstack.flinkx.options.OptionParser;
import com.dtstack.flinkx.options.Options;
import com.dtstack.flinkx.parser.SqlParser;
import com.dtstack.flinkx.sink.BaseDataSink;
import com.dtstack.flinkx.sink.DataSinkFactory;
import com.dtstack.flinkx.source.BaseDataSource;
import com.dtstack.flinkx.source.DataSourceFactory;
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

import static com.dtstack.flinkx.constants.ConfigConstant.STRATEGY_DELAYINTERVAL;
import static com.dtstack.flinkx.constants.ConfigConstant.STRATEGY_FAILUREINTERVAL;
import static com.dtstack.flinkx.constants.ConfigConstant.STRATEGY_FAILURERATE;
import static com.dtstack.flinkx.constants.ConfigConstant.STRATEGY_RESTARTATTEMPTS;
import static com.dtstack.flinkx.constants.ConfigConstant.STRATEGY_STRATEGY;

/**
 * The main class entry
 * <p>
 * Company: www.dtstack.com
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
                throw new RuntimeException("just support sql or sync jobType !!!");
        }

        LOG.info("program {} execution success", options.getJobName());
    }

    /**
     * 执行sql 类型任务
     * @param env
     * @param tableEnv
     * @param job
     * @param options
     * @param confProperties
     * @throws Exception
     */
    private static void exeSqlJob(StreamExecutionEnvironment env,
                                  StreamTableEnvironment tableEnv,
                                  String job,
                                  Options options,
                                  Properties confProperties) throws Exception {
        configStreamExecutionEnvironment(env, options, null, confProperties);
        List<URL> jarUrlList = ExecuteProcessHelper.getExternalJarUrls(options.getAddjar());
        StatementSet statementSet = SqlParser.parseSql(job, jarUrlList, tableEnv);
        statementSet.execute();
    }

    /**
     * 执行 数据同步类型任务
     * @param env
     * @param tableEnv
     * @param job
     * @param options
     * @param confProperties
     * @throws Exception
     */
    private static void exeSyncJob(StreamExecutionEnvironment env,
                                   StreamTableEnvironment tableEnv,
                                   String job,
                                   Options options,
                                   Properties confProperties) throws Exception {
        SyncConf config = parseFlinkxConf(job, options);
        buildRestartStrategy(confProperties, config);
        configStreamExecutionEnvironment(env, options, config, confProperties);

        BaseDataSource dataReader = DataSourceFactory.getDataSource(config, env);
        DataStream<RowData> sourceDataStream = dataReader.readData();

        SpeedConf speed = config.getSpeed();
        if (speed.getReaderChannel() > 0) {
            sourceDataStream = ((DataStreamSource<RowData>) sourceDataStream).setParallelism(speed.getReaderChannel());
        }

        DataStream<RowData> dataStream;
        boolean transformer = config.getTransformer() != null
                && StringUtils.isNotBlank(config.getTransformer().getTransformSql());

        if (transformer) {
            String fieldNames = String.join(",", config.getReader().getFieldNameList());
            List<Expression> expressionList = ExpressionParser.parseExpressionList(fieldNames);
            Table sourceTable = tableEnv.fromDataStream(sourceDataStream, expressionList.toArray(new Expression[0]));
            tableEnv.createTemporaryView(config.getReader().getTable().getTableName(), sourceTable);

            Table adaptTable = tableEnv.sqlQuery(config
                    .getJob()
                    .getTransformer()
                    .getTransformSql());
            TypeInformation<RowData> typeInformation = TableUtil.getTypeInformation(
                    adaptTable.getSchema().getFieldDataTypes(),
                    adaptTable.getSchema().getFieldNames());
            dataStream = tableEnv
                    .toRetractStream(adaptTable, typeInformation)
                    .map(f -> f.f1);
            tableEnv.createTemporaryView(
                    config.getWriter().getTable().getTableName(),
                    dataStream);
        } else {
            dataStream = sourceDataStream;
        }

        if (speed.isRebalance()) {
            dataStream = dataStream.rebalance();
        }

        BaseDataSink dataWriter = DataSinkFactory.getDataSink(config);

        DataStream casted;
        LogicalType sourceTypes = dataReader.getLogicalType();
        LogicalType targetTypes = dataWriter.getLogicalType();
        // 任意一端拿不到类型则不启动转换函数
        if (sourceTypes != null && targetTypes != null) {
            CastFunction cast = new CastFunction(sourceTypes, targetTypes);
            cast.init();
            casted = dataStream.map(cast);
        } else {
            casted = dataStream;
        }

        // TODO 添加转换算子或者转换函数
        DataStreamSink<RowData> dataStreamSink = dataWriter.writeData(casted);
        if (speed.getWriterChannel() > 0) {
            dataStreamSink.setParallelism(speed.getWriterChannel());
        }

        JobExecutionResult result = env.execute(options.getJobName());
        if (env instanceof MyLocalStreamEnvironment) {
            PrintUtil.printResult(result);
        }
    }

    /**
     * 将配置文件中的任务重启策略设置到Properties中
     *
     * @param confProperties
     * @param config
     */
    private static void buildRestartStrategy(Properties confProperties, SyncConf config) {
        RestartConf restart = config.getRestart();
        confProperties.setProperty(STRATEGY_STRATEGY, restart.getStrategy());
        confProperties.setProperty(STRATEGY_RESTARTATTEMPTS, String.valueOf(restart.getRestartAttempts()));
        confProperties.setProperty(STRATEGY_DELAYINTERVAL, String.valueOf(restart.getDelayInterval()));
        confProperties.setProperty(STRATEGY_FAILURERATE, String.valueOf(restart.getFailureRate()));
        confProperties.setProperty(STRATEGY_FAILUREINTERVAL, String.valueOf(restart.getFailureInterval()));
    }

    /**
     * 解析并设置job
     *
     * @param job
     * @param options
     *
     * @return
     */
    public static SyncConf parseFlinkxConf(String job, Options options) {
        SyncConf config;
        try {
            config = SyncConf.parseJob(job);

            if (StringUtils.isNotEmpty(options.getPluginRoot())) {
                config.setPluginRoot(options.getPluginRoot());
            }

            if (StringUtils.isNotEmpty(options.getRemotePluginPath())) {
                config.setRemotePluginPath(options.getRemotePluginPath());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return config;
    }

    /**
     * 创建StreamExecutionEnvironment
     *
     * @param options
     *
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
     *
     * @return
     */
    private static StreamTableEnvironment createStreamTableEnvironment(StreamExecutionEnvironment env) {
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
     *
     * @param env StreamExecutionEnvironment
     * @param options options
     * @param config FlinkxConf
     * @param properties confProperties
     */
    private static void configStreamExecutionEnvironment(
            StreamExecutionEnvironment env,
            Options options,
            SyncConf config,
            Properties properties) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, IOException {
        configRestartStrategy(env, properties);
        configCheckpoint(env, properties);
        configEnvironment(env, properties);
        if (config != null) {
            PluginUtil.registerPluginUrlToCachedFile(config, env);
            env.setParallelism(config.getSpeed().getChannel());
            if (env instanceof MyLocalStreamEnvironment) {
                if (StringUtils.isNotEmpty(options.getS())) {
                    ((MyLocalStreamEnvironment) env).setSettings(SavepointRestoreSettings.forPath(
                            options.getS()));
                }
            }
        } else {
            Preconditions.checkArgument(
                    ExecuteProcessHelper.checkRemoteSqlPluginPath(
                            options.getRemotePluginPath(),
                            options.getMode(),
                            options.getPluginLoadMode()),
                    "Non-local mode or shipfile deployment mode, remoteSqlPluginPath is required");
            FactoryUtil.setPluginPath(StringUtils.isNotEmpty(options.getPluginRoot()) ? options.getPluginRoot() : options
                    .getRemotePluginPath());
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
    private static void configStreamExecutionEnvironment(
            StreamTableEnvironment tableEnv,
            Properties properties,
            String jobName) {
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
    private static void configRestartStrategy(
            StreamExecutionEnvironment env,
            Properties properties) {
        if (ConfigConstant.STRATEGY_FIXED_DELAY.equalsIgnoreCase(properties.getProperty(
                STRATEGY_STRATEGY))) {
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                    MathUtil.getIntegerVal(properties.getProperty(STRATEGY_RESTARTATTEMPTS)),
                    Time.of(
                            MathUtil.getLongVal(properties.getProperty(STRATEGY_DELAYINTERVAL)),
                            TimeUnit.SECONDS)
            ));
        } else if (ConfigConstant.STRATEGY_FAILURE_RATE.equalsIgnoreCase(properties.getProperty(
                STRATEGY_STRATEGY)) || StreamEnvConfigManagerUtil.isRestore(properties).get()) {
            env.setRestartStrategy(RestartStrategies.failureRateRestart(
                    MathUtil.getIntegerVal(
                            properties.getProperty(STRATEGY_FAILURERATE),
                            ConfigConstant.FAILUEE_RATE),
                    Time.of(
                            MathUtil.getLongVal(
                                    properties.getProperty(STRATEGY_FAILUREINTERVAL),
                                    StreamEnvConfigManagerUtil
                                            .getFailureInterval(properties)
                                            .get()),
                            TimeUnit.SECONDS),
                    Time.of(
                            MathUtil.getLongVal(
                                    properties.getProperty(STRATEGY_DELAYINTERVAL),
                                    StreamEnvConfigManagerUtil.getDelayInterval(properties).get()),
                            TimeUnit.SECONDS)
            ));
        } else {
            env.setRestartStrategy(RestartStrategies.noRestart());
        }
    }

    /**
     * 配置最大并行度等相关参数
     *
     * @param streamEnv
     * @param confProperties
     *
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     */
    private static void configEnvironment(
            StreamExecutionEnvironment streamEnv,
            Properties confProperties) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        confProperties = PropertiesUtil.propertiesTrim(confProperties);
        streamEnv.getConfig().disableClosureCleaner();

        Configuration globalJobParameters = new Configuration();
        //Configuration unsupported set properties key-value
        Method method = Configuration.class.getDeclaredMethod(
                "setValueInternal",
                String.class,
                Object.class);
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

        StreamEnvConfigManagerUtil
                .getEnvParallelism(confProperties)
                .ifPresent(streamEnv::setParallelism);
        StreamEnvConfigManagerUtil
                .getMaxEnvParallelism(confProperties)
                .ifPresent(streamEnv::setMaxParallelism);
        StreamEnvConfigManagerUtil
                .getBufferTimeoutMillis(confProperties)
                .ifPresent(streamEnv::setBufferTimeout);
        StreamEnvConfigManagerUtil
                .getStreamTimeCharacteristic(confProperties)
                .ifPresent(streamEnv::setStreamTimeCharacteristic);
        StreamEnvConfigManagerUtil.getAutoWatermarkInterval(confProperties).ifPresent(op -> {
            if (streamEnv.getStreamTimeCharacteristic().equals(TimeCharacteristic.EventTime)) {
                streamEnv.getConfig().setAutoWatermarkInterval(op);
            }
        });
    }

    /**
     * flink任务设置checkpoint
     *
     * @param env
     * @param properties
     *
     * @return
     */
    private static void configCheckpoint(
            StreamExecutionEnvironment env,
            Properties properties) throws IOException {
        Optional<Boolean> checkpointEnabled = StreamEnvConfigManagerUtil.isCheckpointEnabled(
                properties);
        if (checkpointEnabled.get()) {
            StreamEnvConfigManagerUtil
                    .getCheckpointInterval(properties)
                    .ifPresent(env::enableCheckpointing);
            StreamEnvConfigManagerUtil
                    .getCheckpointMode(properties)
                    .ifPresent(env.getCheckpointConfig()::setCheckpointingMode);
            StreamEnvConfigManagerUtil
                    .getCheckpointTimeout(properties)
                    .ifPresent(env.getCheckpointConfig()::setCheckpointTimeout);
            StreamEnvConfigManagerUtil
                    .getMaxConcurrentCheckpoints(properties)
                    .ifPresent(env.getCheckpointConfig()::setMaxConcurrentCheckpoints);
            StreamEnvConfigManagerUtil
                    .getCheckpointCleanup(properties)
                    .ifPresent(env.getCheckpointConfig()::enableExternalizedCheckpoints);
            StreamEnvConfigManagerUtil
                    .enableUnalignedCheckpoints(properties)
                    .ifPresent(event -> env
                            .getCheckpointConfig()
                            .enableUnalignedCheckpoints(event));
            StreamEnvConfigManagerUtil.getStateBackend(properties).ifPresent(env::setStateBackend);
        }
    }
}
