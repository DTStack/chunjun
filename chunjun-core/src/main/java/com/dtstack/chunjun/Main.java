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
package com.dtstack.chunjun;

import com.dtstack.chunjun.cdc.CdcConfig;
import com.dtstack.chunjun.cdc.RestorationFlatMap;
import com.dtstack.chunjun.cdc.ddl.DdlConvent;
import com.dtstack.chunjun.cdc.handler.CacheHandler;
import com.dtstack.chunjun.cdc.handler.DDLHandler;
import com.dtstack.chunjun.config.OperatorConfig;
import com.dtstack.chunjun.config.SpeedConfig;
import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.dirty.DirtyConf;
import com.dtstack.chunjun.dirty.utils.DirtyConfUtil;
import com.dtstack.chunjun.enums.ClusterMode;
import com.dtstack.chunjun.enums.EJobType;
import com.dtstack.chunjun.environment.EnvFactory;
import com.dtstack.chunjun.environment.MyLocalStreamEnvironment;
import com.dtstack.chunjun.mapping.MappingConf;
import com.dtstack.chunjun.mapping.NameMappingFlatMap;
import com.dtstack.chunjun.options.OptionParser;
import com.dtstack.chunjun.options.Options;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.sql.parser.SqlParser;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.JobConfigException;
import com.dtstack.chunjun.util.DataSyncFactoryUtil;
import com.dtstack.chunjun.util.ExecuteProcessHelper;
import com.dtstack.chunjun.util.FactoryHelper;
import com.dtstack.chunjun.util.JobUtil;
import com.dtstack.chunjun.util.PluginUtil;
import com.dtstack.chunjun.util.PrintUtil;
import com.dtstack.chunjun.util.PropertiesUtil;
import com.dtstack.chunjun.util.RealTimeDataSourceNameUtil;
import com.dtstack.chunjun.util.TableUtil;

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
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.Expression;
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
import java.util.stream.Collectors;

public class Main {

    public static Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        LOG.info("------------program params-------------------------");
        Arrays.stream(args).forEach(arg -> LOG.info("{}", arg));
        LOG.info("-------------------------------------------");

        Options options = new OptionParser(args).getOptions();
        String job = URLDecoder.decode(options.getJob(), StandardCharsets.UTF_8.name());
        String replacedJob = JobUtil.replaceJobParameter(options.getP(), options.getPj(), job);
        Properties confProperties = PropertiesUtil.parseConf(options.getConfProp());
        StreamExecutionEnvironment env = EnvFactory.createStreamExecutionEnvironment(options);
        StreamTableEnvironment tEnv =
                EnvFactory.createStreamTableEnvironment(env, confProperties, options.getJobName());
        LOG.info(
                "Register to table configuration:{}",
                tEnv.getConfig().getConfiguration().toString());
        switch (EJobType.getByName(options.getJobType())) {
            case SQL:
                exeSqlJob(env, tEnv, replacedJob, options);
                break;
            case SYNC:
                exeSyncJob(env, tEnv, replacedJob, options);
                break;
            default:
                throw new ChunJunRuntimeException(
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
            throw new ChunJunRuntimeException(e);
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
        SyncConfig config = parseConf(job, options);
        configStreamExecutionEnvironment(env, options, config);

        SourceFactory sourceFactory = DataSyncFactoryUtil.discoverSource(config, env);
        DataStream<RowData> dataStreamSource = sourceFactory.createSource();
        SpeedConfig speed = config.getSpeed();
        if (speed.getReaderChannel() > 0) {
            dataStreamSource =
                    ((DataStreamSource<RowData>) dataStreamSource)
                            .setParallelism(speed.getReaderChannel());
        }

        dataStreamSource = addMappingOperator(config, dataStreamSource);

        if (null != config.getCdcConf()
                && (null != config.getCdcConf().getDdl()
                        && null != config.getCdcConf().getCache())) {
            CdcConfig cdcConfig = config.getCdcConf();
            DDLHandler ddlHandler = DataSyncFactoryUtil.discoverDdlHandler(cdcConfig, config);

            CacheHandler cacheHandler = DataSyncFactoryUtil.discoverCacheHandler(cdcConfig, config);
            dataStreamSource =
                    dataStreamSource.flatMap(
                            new RestorationFlatMap(ddlHandler, cacheHandler, cdcConfig));
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
            SyncConfig config,
            DataStream<RowData> sourceDataStream) {
        List<Expression> expressionList =
                config.getReader().getFieldNameList().stream()
                        .map(ApiExpressionUtils::unresolvedRef)
                        .collect(Collectors.toList());
        Table sourceTable =
                tableEnv.fromDataStream(
                        sourceDataStream, expressionList.toArray(new Expression[0]));

        checkTableConf(config.getReader());
        tableEnv.createTemporaryView(config.getReader().getTable().getTableName(), sourceTable);

        String transformSql = config.getJob().getTransformer().getTransformSql();
        Table adaptTable = tableEnv.sqlQuery(transformSql);

        DataType[] tableDataTypes = adaptTable.getSchema().getFieldDataTypes();
        String[] tableFieldNames = adaptTable.getSchema().getFieldNames();
        TypeInformation<? extends RowData> typeInformation =
                TableUtil.getTypeInformation(tableDataTypes, tableFieldNames);
        DataStream<RowData> dataStream =
                tableEnv.toRetractStream(adaptTable, typeInformation).map(f -> f.f1);

        checkTableConf(config.getWriter());
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
    public static SyncConfig parseConf(String job, Options options) {
        SyncConfig config;
        try {
            config = SyncConfig.parseJob(job);

            // 设置chunjun-dist的路径
            if (StringUtils.isNotBlank(options.getChunjunDistDir())) {
                config.setPluginRoot(options.getChunjunDistDir());
            }

            Properties confProperties = PropertiesUtil.parseConf(options.getConfProp());

            String savePointPath =
                    confProperties.getProperty(SavepointConfigOptions.SAVEPOINT_PATH.key());
            if (StringUtils.isNotBlank(savePointPath)) {
                config.setSavePointPath(savePointPath);
            }

        } catch (Exception e) {
            throw new ChunJunRuntimeException(e);
        }
        return config;
    }

    /**
     * 配置StreamExecutionEnvironment
     *
     * @param env StreamExecutionEnvironment
     * @param options options
     * @param config ChunJunConf
     */
    private static void configStreamExecutionEnvironment(
            StreamExecutionEnvironment env, Options options, SyncConfig config) {

        if (config != null) {
            PluginUtil.registerPluginUrlToCachedFile(options, config, env);
            env.setParallelism(config.getSpeed().getChannel());
        } else {
            Preconditions.checkArgument(
                    ExecuteProcessHelper.checkRemoteSqlPluginPath(
                            options.getRemoteChunJunDistDir(),
                            options.getMode(),
                            options.getPluginLoadMode()),
                    "Non-local mode or shipfile deployment mode, remoteSqlPluginPath is required");
            FactoryHelper factoryHelper = new FactoryHelper();
            factoryHelper.setLocalPluginPath(options.getChunjunDistDir());
            factoryHelper.setRemotePluginPath(options.getRemoteChunJunDistDir());
            factoryHelper.setPluginLoadMode(options.getPluginLoadMode());
            factoryHelper.setEnv(env);
            factoryHelper.setExecutionMode(options.getMode());
            DirtyConf dirtyConf = DirtyConfUtil.parse(options);
            // 注册core包
            if (ClusterMode.local.name().equalsIgnoreCase(options.getMode())) {
                factoryHelper.registerCachedFile(
                        "", Thread.currentThread().getContextClassLoader(), "");
            }

            factoryHelper.registerCachedFile(
                    dirtyConf.getType(),
                    Thread.currentThread().getContextClassLoader(),
                    ConstantValue.DIRTY_DATA_DIR_NAME);
            // TODO sql 支持restore.

            FactoryUtil.setFactoryUtilHelp(factoryHelper);
            TableFactoryService.setFactoryUtilHelp(factoryHelper);
        }
        PluginUtil.registerShipfileToCachedFile(options.getAddShipfile(), env);
    }

    /**
     * Check required config item.
     *
     * @param operatorConfig
     */
    private static void checkTableConf(OperatorConfig operatorConfig) {
        if (operatorConfig.getTable() == null) {
            throw new JobConfigException(operatorConfig.getName(), "table", "is missing");
        }
        if (StringUtils.isEmpty(operatorConfig.getTable().getTableName())) {
            throw new JobConfigException(operatorConfig.getName(), "table.tableName", "is missing");
        }
    }

    private static DataStream<RowData> addMappingOperator(
            SyncConfig config, DataStream<RowData> dataStreamSource) {

        String sourceName =
                RealTimeDataSourceNameUtil.getDataSourceName(
                        PluginUtil.replaceReaderAndWriterSuffix(config.getReader().getName()));
        // if source is kafka, need to specify the data source in mappingConf
        if (config.getNameMappingConf() != null
                && StringUtils.isNotBlank(config.getNameMappingConf().getSourceName())) {
            sourceName = config.getNameMappingConf().getSourceName();
        }
        // 如果是实时任务 则sourceName 会和脚本里的名称不一致 例如 oraclelogminer 会转为oracle，binlogreader转为mysql
        if (PluginUtil.replaceReaderAndWriterSuffix(config.getReader().getName())
                .equals(sourceName)) {
            return dataStreamSource;
        }
        String sinkName = PluginUtil.replaceReaderAndWriterSuffix(config.getWriter().getName());

        // 异构数据源 或者 需要进行元数据替换
        boolean ddlSkip = config.getReader().getBooleanVal("ddlSkip", true);
        boolean useDdlConvent =
                !sourceName.equals(sinkName) && !ddlSkip
                        || (config.getNameMappingConf() != null
                                && config.getNameMappingConf().needReplaceMetaData());

        if (useDdlConvent) {
            DdlConvent sourceDdlConvent = null;
            DdlConvent sinkDdlConvent = null;
            MappingConf mappingConf = config.getNameMappingConf();

            try {
                sourceDdlConvent =
                        DataSyncFactoryUtil.discoverDdlConventHandler(
                                mappingConf, sourceName, config);
            } catch (Throwable e) {
                // ignore
            }

            if (sourceDdlConvent == null || sourceName.equals(sinkName)) {
                sinkDdlConvent = sourceDdlConvent;
            } else {
                try {
                    sinkDdlConvent =
                            DataSyncFactoryUtil.discoverDdlConventHandler(
                                    mappingConf, sinkName, config);
                } catch (Throwable e) {
                    // ignore
                }
            }

            return dataStreamSource.flatMap(
                    new NameMappingFlatMap(
                            mappingConf, useDdlConvent, sourceDdlConvent, sinkDdlConvent));
        }
        return dataStreamSource;
    }
}
