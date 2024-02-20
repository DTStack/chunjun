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
import com.dtstack.chunjun.constants.Metrics;
import com.dtstack.chunjun.dirty.DirtyConfig;
import com.dtstack.chunjun.dirty.utils.DirtyConfUtil;
import com.dtstack.chunjun.enums.ClusterMode;
import com.dtstack.chunjun.enums.EJobType;
import com.dtstack.chunjun.environment.EnvFactory;
import com.dtstack.chunjun.environment.MyLocalStreamEnvironment;
import com.dtstack.chunjun.mapping.MappingConfig;
import com.dtstack.chunjun.mapping.NameMappingFlatMap;
import com.dtstack.chunjun.options.OptionParser;
import com.dtstack.chunjun.options.Options;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.sql.parser.SqlParser;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.JobConfigException;
import com.dtstack.chunjun.util.DataSyncFactoryUtil;
import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.ExecuteProcessHelper;
import com.dtstack.chunjun.util.FactoryHelper;
import com.dtstack.chunjun.util.JobUtil;
import com.dtstack.chunjun.util.PluginUtil;
import com.dtstack.chunjun.util.PrintUtil;
import com.dtstack.chunjun.util.PropertiesUtil;
import com.dtstack.chunjun.util.RealTimeDataSourceNameUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.dtstack.chunjun.enums.EJobType.SQL;
import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
public class Main {

    public static void main(String[] args) throws Exception {
        log.info("------------program params-------------------------");
        Arrays.stream(args).forEach(arg -> log.info("{}", arg));
        log.info("-------------------------------------------");

        Options options = new OptionParser(args).getOptions();
        String replacedJob = "";
        File file = new File(options.getJob());
        if (file.isFile()) {
            try {
                replacedJob = FileUtils.readFileToString(file, StandardCharsets.UTF_8.name());
            } catch (IOException ioe) {
                log.error("Can not get the job info !!!", ioe);
                throw new RuntimeException(ioe);
            }
        } else {
            String job = URLDecoder.decode(options.getJob(), StandardCharsets.UTF_8.name());
            replacedJob = JobUtil.replaceJobParameter(options.getP(), job);
        }
        Properties confProperties = PropertiesUtil.parseConf(options.getConfProp());
        if (EJobType.getByName(options.getJobType()).equals(SQL)) {
            options.setSqlSetConfiguration(SqlParser.parseSqlSet(replacedJob));
        }
        StreamExecutionEnvironment env = EnvFactory.createStreamExecutionEnvironment(options);
        StreamTableEnvironment tEnv =
                EnvFactory.createStreamTableEnvironment(env, confProperties, options.getJobName());
        log.info(
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

        log.info("program {} execution success", options.getJobName());
    }

    private static void exeSqlJob(
            StreamExecutionEnvironment env,
            StreamTableEnvironment tableEnv,
            String job,
            Options options) {
        try {
            configStreamExecutionEnvironment(env, options, null);
            List<URL> jarUrlList = ExecuteProcessHelper.getExternalJarUrls(options.getAddjar());
            String runMode = options.getRunMode();
            if ("batch".equalsIgnoreCase(runMode)) env.setRuntimeMode(RuntimeExecutionMode.BATCH);
            StatementSet statementSet = SqlParser.parseSql(job, jarUrlList, tableEnv);
            TableResult execute = statementSet.execute();
            // Solve the problem that yarn-per-job sql mode does not exit when executing batch jobs
            Properties confProperties = PropertiesUtil.parseConf(options.getConfProp());
            String executionMode =
                    confProperties.getProperty(
                            "chunjun.cluster.execution-mode",
                            ClusterEntrypoint.ExecutionMode.DETACHED.name());
            if (!ClusterEntrypoint.ExecutionMode.DETACHED.name().equalsIgnoreCase(executionMode)) {
                // wait job finish
                printSqlResult(execute);
            }
            if (env instanceof MyLocalStreamEnvironment) {
                Optional<JobClient> jobClient = execute.getJobClient();
                if (jobClient.isPresent()) {
                    PrintUtil.printResult(
                            jobClient
                                    .get()
                                    .getJobExecutionResult()
                                    .get()
                                    .getAllAccumulatorResults());
                }
            }
        } catch (Exception e) {
            throw new ChunJunRuntimeException(e);
        }
    }

    private static void exeSyncJob(
            StreamExecutionEnvironment env,
            StreamTableEnvironment tableEnv,
            String job,
            Options options)
            throws Exception {
        SyncConfig config = parseConfig(job, options);
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

    private static DataStream<RowData> syncStreamToTable(
            StreamTableEnvironment tableEnv,
            SyncConfig config,
            DataStream<RowData> sourceDataStream) {
        Schema.Builder builder = Schema.newBuilder();
        for (RowType.RowField rowField :
                ((InternalTypeInfo<?>) sourceDataStream.getType()).toRowType().getFields()) {
            builder.column(rowField.getName(), rowField.getType().asSerializableString());
        }

        Table sourceTable = tableEnv.fromDataStream(sourceDataStream, builder.build());

        checkTableConfig(config.getReader());
        tableEnv.createTemporaryView(config.getReader().getTable().getTableName(), sourceTable);

        String transformSql = config.getJob().getTransformer().getTransformSql();
        Table adaptTable = tableEnv.sqlQuery(transformSql);

        DataType[] tableDataTypes = adaptTable.getSchema().getFieldDataTypes();
        String[] tableFieldNames = adaptTable.getSchema().getFieldNames();
        TypeInformation<? extends RowData> typeInformation =
                TableUtil.getTypeInformation(tableDataTypes, tableFieldNames);
        DataStream<RowData> dataStream =
                tableEnv.toRetractStream(adaptTable, typeInformation).map(f -> f.f1);

        checkTableConfig(config.getWriter());
        tableEnv.createTemporaryView(config.getWriter().getTable().getTableName(), dataStream);

        return dataStream;
    }

    public static SyncConfig parseConfig(String job, Options options) {
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
            DirtyConfig dirtyConfig = DirtyConfUtil.parse(options);
            // 注册core包
            if (ClusterMode.local.name().equalsIgnoreCase(options.getMode())) {
                factoryHelper.registerCachedFile(
                        "", Thread.currentThread().getContextClassLoader(), "");
            }

            factoryHelper.registerCachedFile(
                    dirtyConfig.getType(),
                    Thread.currentThread().getContextClassLoader(),
                    ConstantValue.DIRTY_DATA_DIR_NAME);
            // TODO sql 支持restore.
            FactoryUtil.setFactoryHelper(factoryHelper);
        }
        PluginUtil.registerShipfileToCachedFile(options.getAddShipfile(), env);
    }

    private static void checkTableConfig(OperatorConfig operatorConfig) {
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
        if (config.getNameMappingConfig() != null
                && StringUtils.isNotBlank(config.getNameMappingConfig().getSourceName())) {
            sourceName = config.getNameMappingConfig().getSourceName();
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
                        || (config.getNameMappingConfig() != null
                                && config.getNameMappingConfig().needReplaceMetaData());

        if (useDdlConvent) {
            DdlConvent sourceDdlConvent = null;
            DdlConvent sinkDdlConvent = null;
            MappingConfig mappingConfig = config.getNameMappingConfig();

            try {
                sourceDdlConvent =
                        DataSyncFactoryUtil.discoverDdlConventHandler(
                                mappingConfig, sourceName, config);
            } catch (Throwable e) {
                // ignore
            }

            if (sourceDdlConvent == null || sourceName.equals(sinkName)) {
                sinkDdlConvent = sourceDdlConvent;
            } else {
                try {
                    sinkDdlConvent =
                            DataSyncFactoryUtil.discoverDdlConventHandler(
                                    mappingConfig, sinkName, config);
                } catch (Throwable e) {
                    // ignore
                }
            }

            return dataStreamSource.flatMap(
                    new NameMappingFlatMap(
                            mappingConfig, useDdlConvent, sourceDdlConvent, sinkDdlConvent));
        }
        return dataStreamSource;
    }

    /**
     * Solve the problem that the execution of yarn-per-job sql mode does not exit. Solve the
     * problem of obtaining statistical indicators and reporting errors when the degree of
     * parallelism is large
     */
    private static void printSqlResult(TableResult execute) {
        Optional<JobClient> jobClient = execute.getJobClient();
        jobClient.ifPresent(
                v -> {
                    Map<String, Object> accumulators = null;
                    int sleepTime = 15000;
                    CompletableFuture<JobExecutionResult> jobExecutionResult = null;
                    String msg = "";
                    int tryNum = 0;
                    while (true) {
                        try {
                            Thread.sleep(sleepTime);
                            ApplicationStatus applicationStatus =
                                    ApplicationStatus.fromJobStatus(v.getJobStatus().get());
                            String status = applicationStatus.toString();
                            switch (applicationStatus) {
                                case FAILED:
                                    msg = "Failed to execute this sql";
                                    break;
                                case CANCELED:
                                    msg = "Canceled to execute this sql";
                                    break;
                                case SUCCEEDED:
                                    msg = "Succeeded to execute this sql";
                                    break;
                                default:
                            }
                            if (StringUtils.isNotBlank(status)) {
                                status = status.toUpperCase();
                                if (status.contains("FAILED")) {
                                    msg = "Failed to execute this sql";
                                }
                            }
                            if (StringUtils.isNotBlank(msg)) {
                                accumulators = v.getAccumulators().get();
                                if (null != accumulators.get(Metrics.READ_BYTES)) {
                                    Map<String, Object> data = new LinkedHashMap<>();
                                    for (String key : Metrics.METRIC_SINK_LIST) {
                                        data.put(key, accumulators.get(key));
                                    }
                                    data.put(
                                            Metrics.READ_BYTES,
                                            accumulators.get(Metrics.READ_BYTES));
                                    data.put(
                                            Metrics.READ_DURATION,
                                            accumulators.get(Metrics.READ_DURATION));
                                    data.put(
                                            Metrics.SNAPSHOT_WRITES,
                                            accumulators.get(Metrics.SNAPSHOT_WRITES));
                                    System.out.println(PrintUtil.printResult(data));
                                } else {
                                    System.out.println(
                                            getDateTime() + " accumulator read bytes is null");
                                }
                                System.out.println(
                                        getDateTime()
                                                + " Start to stop flink job("
                                                + v.getJobID()
                                                + ")");
                                while (true) {
                                    try {
                                        jobExecutionResult = v.getJobExecutionResult();
                                        jobExecutionResult.complete(
                                                new JobExecutionResult(v.getJobID(), 0, null));
                                        ApplicationStatus.fromJobStatus(
                                                v.getJobStatus().get(3, SECONDS));
                                        Thread.sleep(2000);
                                    } catch (Exception e) {
                                        break;
                                    }
                                }
                                System.out.println(
                                        getDateTime()
                                                + " Success to stop flink job("
                                                + v.getJobID()
                                                + ")");
                                int code = applicationStatus.processExitCode();
                                System.out.println(
                                        getDateTime() + " Flink process exit code is: " + code);
                                System.exit(code);
                            }
                            accumulators = v.getAccumulators().get();
                            if (null == accumulators.get(Metrics.READ_BYTES)) {
                                continue;
                            }
                        } catch (ExecutionException e) {
                            // Handle getAccumulators exception
                            tryNum++;
                            System.out.println(
                                    getDateTime()
                                            + " ERROR ["
                                            + tryNum
                                            + " times] "
                                            + e.getMessage());
                            // Increase the maximum number of attempts limit
                            if (tryNum > 2) {
                                System.out.println(
                                        getDateTime()
                                                + " try get accumulators num has reached the limit");
                                System.out.println(ExceptionUtil.getErrorMessage(e));
                                System.exit(-1);
                            }
                        } catch (Exception e) {
                            System.out.println(getDateTime() + " -----------------------------");
                            System.out.println(ExceptionUtil.getErrorMessage(e));
                            System.out.println(
                                    getDateTime()
                                            + " yarn task maybe killed, error to execute this sql");
                            System.exit(-1);
                        }
                    }
                });
    }

    public static String getDateTime() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return dateFormat.format(new Date());
    }
}
