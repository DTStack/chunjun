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

import com.dtstack.flinkx.conf.SpeedConf;
import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.dirty.DirtyConf;
import com.dtstack.flinkx.dirty.utils.DirtyConfUtil;
import com.dtstack.flinkx.enums.EJobType;
import com.dtstack.flinkx.environment.EnvFactory;
import com.dtstack.flinkx.environment.MyLocalStreamEnvironment;
import com.dtstack.flinkx.options.OptionParser;
import com.dtstack.flinkx.options.Options;
import com.dtstack.flinkx.sink.SinkFactory;
import com.dtstack.flinkx.source.SourceFactory;
import com.dtstack.flinkx.sql.parser.SqlParser;
import com.dtstack.flinkx.throwable.FlinkxRuntimeException;
import com.dtstack.flinkx.util.DataSyncFactoryUtil;
import com.dtstack.flinkx.util.ExecuteProcessHelper;
import com.dtstack.flinkx.util.FactoryHelper;
import com.dtstack.flinkx.util.PluginUtil;
import com.dtstack.flinkx.util.PrintUtil;
import com.dtstack.flinkx.util.PropertiesUtil;
import com.dtstack.flinkx.util.TableUtil;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionParser;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.types.DataType;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

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
        String job = URLDecoder.decode(options.getJob(), StandardCharsets.UTF_8.name());
        Properties confProperties = PropertiesUtil.parseConf(options.getConfProp());
        StreamExecutionEnvironment env = EnvFactory.createStreamExecutionEnvironment(options);
        StreamTableEnvironment tEnv =
                EnvFactory.createStreamTableEnvironment(env, confProperties, options.getJobName());

        switch (EJobType.getByName(options.getJobType())) {
            case SQL:
                exeSqlJob(env, tEnv, job, options);
                break;
            case SYNC:
                exeSyncJob(env, tEnv, job, options);
                break;
            default:
                throw new FlinkxRuntimeException(
                        "unknown jobType: ["
                                + options.getJobType()
                                + "], jobType must in [SQL, SYNC].");
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
     * @throws Exception
     */
    private static void exeSqlJob(
            StreamExecutionEnvironment env,
            StreamTableEnvironment tableEnv,
            String job,
            Options options) {
        try {
            configStreamExecutionEnvironment(env, options, null);
            List<URL> jarUrlList = ExecuteProcessHelper.getExternalJarUrls(options.getAddjar());
            StatementSet statementSet = SqlParser.parseSql(job, jarUrlList, tableEnv);
            TableResult execute = statementSet.execute();
            if (env instanceof MyLocalStreamEnvironment) {
                Optional<JobClient> jobClient = execute.getJobClient();
                if (jobClient.isPresent()) {
                    PrintUtil.printResult(jobClient.get().getAccumulators().get());
                }
            }
        } catch (Exception e) {
            throw new FlinkxRuntimeException(e);
        } finally {
            FactoryUtil.getFactoryHelperThreadLocal().remove();
            TableFactoryService.getFactoryHelperThreadLocal().remove();
        }
    }

    /**
     * 执行 数据同步类型任务
     *
     * @param env
     * @param tableEnv
     * @param job
     * @param options
     * @throws Exception
     */
    private static void exeSyncJob(
            StreamExecutionEnvironment env,
            StreamTableEnvironment tableEnv,
            String job,
            Options options)
            throws Exception {
        SyncConf config = parseFlinkxConf(job, options);
        configStreamExecutionEnvironment(env, options, config);

        SourceFactory sourceFactory = DataSyncFactoryUtil.discoverSource(config, env);
        DataStream<RowData> dataStreamSource = sourceFactory.createSource();

        SpeedConf speed = config.getSpeed();
        if (speed.getReaderChannel() > 0) {
            dataStreamSource =
                    ((DataStreamSource<RowData>) dataStreamSource)
                            .setParallelism(speed.getReaderChannel());
        }

        DataStream<RowData> dataStream;
        boolean transformer =
                config.getTransformer() != null
                        && StringUtils.isNotBlank(config.getTransformer().getTransformSql());

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
    private static DataStream<RowData> syncStreamToTable(
            StreamTableEnvironment tableEnv,
            SyncConf config,
            DataStream<RowData> sourceDataStream) {
        String fieldNames =
                String.join(ConstantValue.COMMA_SYMBOL, config.getReader().getFieldNameList());
        List<Expression> expressionList = ExpressionParser.parseExpressionList(fieldNames);
        Table sourceTable =
                tableEnv.fromDataStream(
                        sourceDataStream, expressionList.toArray(new Expression[0]));
        tableEnv.createTemporaryView(config.getReader().getTable().getTableName(), sourceTable);

        String transformSql = config.getJob().getTransformer().getTransformSql();
        Table adaptTable = tableEnv.sqlQuery(transformSql);

        DataType[] tableDataTypes = adaptTable.getSchema().getFieldDataTypes();
        String[] tableFieldNames = adaptTable.getSchema().getFieldNames();
        TypeInformation<RowData> typeInformation =
                TableUtil.getTypeInformation(tableDataTypes, tableFieldNames);
        DataStream<RowData> dataStream =
                tableEnv.toRetractStream(adaptTable, typeInformation).map(f -> f.f1);
        tableEnv.createTemporaryView(config.getWriter().getTable().getTableName(), dataStream);

        return dataStream;
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

            if (StringUtils.isNotBlank(options.getFlinkxDistDir())) {
                config.setPluginRoot(options.getFlinkxDistDir());
            }

            Properties confProperties = PropertiesUtil.parseConf(options.getConfProp());

            String savePointPath =
                    confProperties.getProperty(SavepointConfigOptions.SAVEPOINT_PATH.key());
            if (StringUtils.isNotBlank(savePointPath)) {
                config.setSavePointPath(savePointPath);
            }

            if (StringUtils.isNotBlank(options.getRemoteFlinkxDistDir())) {
                config.setRemotePluginPath(options.getRemoteFlinkxDistDir());
            }
        } catch (Exception e) {
            throw new FlinkxRuntimeException(e);
        }
        return config;
    }

    /**
     * 配置StreamExecutionEnvironment
     *
     * @param env StreamExecutionEnvironment
     * @param options options
     * @param config FlinkxConf
     */
    private static void configStreamExecutionEnvironment(
            StreamExecutionEnvironment env, Options options, SyncConf config) {

        if (config != null) {
            PluginUtil.registerPluginUrlToCachedFile(options, config, env);
            env.setParallelism(config.getSpeed().getChannel());
        } else {
            Preconditions.checkArgument(
                    ExecuteProcessHelper.checkRemoteSqlPluginPath(
                            options.getRemoteFlinkxDistDir(),
                            options.getMode(),
                            options.getPluginLoadMode()),
                    "Non-local mode or shipfile deployment mode, remoteSqlPluginPath is required");
            FactoryHelper factoryHelper = new FactoryHelper();
            factoryHelper.setLocalPluginPath(options.getFlinkxDistDir());
            factoryHelper.setRemotePluginPath(options.getRemoteFlinkxDistDir());
            factoryHelper.setPluginLoadMode(options.getPluginLoadMode());
            factoryHelper.setEnv(env);

            DirtyConf dirtyConf = DirtyConfUtil.parse(options);
            factoryHelper.registerCachedFile(
                    dirtyConf.getType(),
                    Thread.currentThread().getContextClassLoader(),
                    ConstantValue.DIRTY_DATA_DIR_NAME);

            FactoryUtil.setFactoryUtilHelp(factoryHelper);
            TableFactoryService.setFactoryUtilHelp(factoryHelper);
        }
        PluginUtil.registerShipfileToCachedFile(options.getAddShipfile(), env);
    }
}
