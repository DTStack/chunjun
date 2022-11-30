package com.dtstack.chunjun.connector.nebula.table;
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.nebula.conf.NebulaConf;
import com.dtstack.chunjun.connector.nebula.conf.NebulaSSLParam;
import com.dtstack.chunjun.connector.nebula.sink.NebulaDynamicTableSink;
import com.dtstack.chunjun.connector.nebula.sink.NebulaOutputFormat;
import com.dtstack.chunjun.connector.nebula.sink.NebulaOutputFormatBuilder;
import com.dtstack.chunjun.connector.nebula.source.NebulaDynamicTableSource;
import com.dtstack.chunjun.connector.nebula.source.NebulaInputFormat;
import com.dtstack.chunjun.connector.nebula.source.NebulaInputFormatBuilder;
import com.dtstack.chunjun.connector.nebula.utils.NebulaSchemaFamily;
import com.dtstack.chunjun.connector.nebula.utils.WriteMode;
import com.dtstack.chunjun.lookup.conf.LookupConf;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.TableSchemaUtils;

import com.vesoft.nebula.client.graph.data.HostAddress;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Tuple3;

import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.BULK_SIZE;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.CACRT_FILE_PATH;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.CLIENT_CONNECT_TIMEOUT_OPTION;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.CONNECTION_RETRY;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.CRT_FILE_PATH;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.DEFAULT_ALLOW_PART_SUCCESS;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.DEFAULT_ALLOW_READ_FOLLOWER;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.ENABLE_SSL;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.END_TIME;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.EXECUTION_RETRY;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.FETCH_INTERVAL;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.FETCH_SIZE;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.FIX_STRING_LEN;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.GRAPHD_ADDRESSES;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.IDLE_TIME;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.INTERVAL_IDLE;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.IS_NEBULA_RECONECT;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.KEY_FILE_PATH;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.MAX_CONNS_SIZE;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.MIN_CONNS_SIZE;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.PASSWORD;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.READ_TASKS;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.SCHEMA_NAME;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.SCHEMA_TYPE;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.SPACE;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.SSL_PARAM_TYPE;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.SSL_PASSWORD;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.START_TIME;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.STORAGE_ADDRESSES;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.USERNAME;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.VID_TYPE;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.WAIT_IDLE_TIME;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.WRITE_MODE;
import static com.dtstack.chunjun.connector.nebula.conf.NebulaOptions.WRITE_TASKS;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.CAS;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.DELIMITERS;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.EDGE;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.EDGE_TYPE;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.INSERT;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.SELF;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.SUB_DELIMITERS;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.TAG;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.UPSERT;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.VERTEX;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_ASYNC_TIMEOUT;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_MAX_ROWS;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_PERIOD;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_TTL;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_TYPE;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_ERROR_LIMIT;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_FETCH_SIZE;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_MAX_RETRIES;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_PARALLELISM;

/**
 * @author: gaoasi
 * @email: aschaser@163.com
 * @date: 2022/10/31 5:31 下午
 */
public class NebulaDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    /** 通过该值查找具体插件 */
    private static final String IDENTIFIER = "nebula-x";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        Tuple3<TableSchema, NebulaConf, ReadableConfig> tuple3 = commonParse(context);
        String[] fieldNames = tuple3._1().getFieldNames();
        List<FieldConf> columnList = new ArrayList<>(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            FieldConf field = new FieldConf();
            field.setName(fieldNames[i]);
            field.setType(
                    ((RowType) tuple3._1().toRowDataType().getLogicalType())
                            .getTypeAt(i)
                            .asSummaryString());
            field.setIndex(i);
            columnList.add(field);
        }
        tuple3._2().setFields(columnList);
        return new NebulaDynamicTableSink(tuple3._1(), tuple3._2(), getOutputFormatBuilder());
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        Tuple3<TableSchema, NebulaConf, ReadableConfig> tuple3 = commonParse(context);
        return new NebulaDynamicTableSource(
                tuple3._2(),
                getLookupConf(tuple3._3(), context.getObjectIdentifier().getObjectName()),
                tuple3._1(),
                getInputFormatBuilder());
    }

    private Tuple3<TableSchema, NebulaConf, ReadableConfig> commonParse(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        String[] fieldNames = context.getCatalogTable().getSchema().getFieldNames();
        NebulaConf nebulaConf = parseNebulaConfig(config);
        nebulaConf.setColumnNames(Arrays.asList(fieldNames));
        return new Tuple3<>(physicalSchema, nebulaConf, config);
    }

    private LookupConf getLookupConf(ReadableConfig config, String tableName) {
        return LookupConf.build()
                .setTableName(tableName)
                .setPeriod(config.get(LOOKUP_CACHE_PERIOD))
                .setCacheSize(config.get(LOOKUP_CACHE_MAX_ROWS))
                .setCacheTtl(config.get(LOOKUP_CACHE_TTL))
                .setCache(config.get(LOOKUP_CACHE_TYPE))
                .setMaxRetryTimes(config.get(LOOKUP_MAX_RETRIES))
                .setErrorLimit(config.get(LOOKUP_ERROR_LIMIT))
                .setFetchSize(config.get(LOOKUP_FETCH_SIZE))
                .setAsyncTimeout(config.get(LOOKUP_ASYNC_TIMEOUT))
                .setParallelism(config.get(LOOKUP_PARALLELISM));
    }

    private NebulaConf parseNebulaConfig(ReadableConfig config) {
        NebulaConf nebulaConf = new NebulaConf();
        nebulaConf.setStart(config.get(START_TIME));
        nebulaConf.setEnd(config.get(END_TIME));
        nebulaConf.setInterval(config.get(FETCH_INTERVAL));
        nebulaConf.setReadTasks(config.get(READ_TASKS));
        nebulaConf.setWriteTasks(config.get(WRITE_TASKS));
        nebulaConf.setDefaultAllowPartSuccess(config.get(DEFAULT_ALLOW_PART_SUCCESS));
        nebulaConf.setDefaultAllowReadFollower(config.get(DEFAULT_ALLOW_READ_FOLLOWER));
        nebulaConf.setFetchSize(config.get(FETCH_SIZE));
        nebulaConf.setCaCrtFilePath(config.get(CACRT_FILE_PATH));
        nebulaConf.setCrtFilePath(config.get(CRT_FILE_PATH));
        nebulaConf.setEnableSSL(config.get(ENABLE_SSL));
        nebulaConf.setConnectionRetry(config.get(CONNECTION_RETRY));
        nebulaConf.setEntityName(config.get(SCHEMA_NAME));
        nebulaConf.setExecutionRetry(config.get(EXECUTION_RETRY));
        nebulaConf.setKeyFilePath(config.get(KEY_FILE_PATH));
        nebulaConf.setPassword(config.get(PASSWORD));
        nebulaConf.setReconn(config.get(IS_NEBULA_RECONECT));
        String s = config.get(SCHEMA_TYPE);
        switch (s) {
            case TAG:
            case VERTEX:
                nebulaConf.setSchemaType(NebulaSchemaFamily.VERTEX);
                break;
            case EDGE:
            case EDGE_TYPE:
                nebulaConf.setSchemaType(NebulaSchemaFamily.EDGE);
                break;
            default:
                throw new UnsupportedTypeException("unsupported nebula schema type!");
        }
        nebulaConf.setStorageAddresses(parseAddress(config, STORAGE_ADDRESSES));
        nebulaConf.setGraphdAddresses(parseAddress(config, GRAPHD_ADDRESSES));
        nebulaConf.setSpace(config.get(SPACE));
        nebulaConf.setVidType(config.get(VID_TYPE));

        String sslParamType = config.get(SSL_PARAM_TYPE);
        if (StringUtils.isNotBlank(sslParamType)) {
            switch (sslParamType) {
                case SELF:
                    nebulaConf.setSslParamType(NebulaSSLParam.SELF_SIGNED_SSL_PARAM);
                    break;
                case CAS:
                    nebulaConf.setSslParamType(NebulaSSLParam.CA_SIGNED_SSL_PARAM);
                    break;
                default:
                    throw new UnsupportedTypeException("unsupport ssl param Type!");
            }
        }
        nebulaConf.setSslPassword(config.get(SSL_PASSWORD));
        nebulaConf.setTimeout(config.get(CLIENT_CONNECT_TIMEOUT_OPTION));
        nebulaConf.setUsername(config.get(USERNAME));
        nebulaConf.setMaxConnsSize(config.get(MAX_CONNS_SIZE));
        nebulaConf.setMinConnsSize(config.get(MIN_CONNS_SIZE));
        nebulaConf.setIdleTime(config.get(IDLE_TIME));
        nebulaConf.setIntervalIdle(config.get(INTERVAL_IDLE));
        nebulaConf.setWaitTime(config.get(WAIT_IDLE_TIME));
        nebulaConf.setStringLength(config.get(FIX_STRING_LEN));
        nebulaConf.setBatchSize(config.get(BULK_SIZE));
        String mode = config.get(WRITE_MODE);
        switch (mode) {
            case INSERT:
                nebulaConf.setMode(WriteMode.INSERT);
                break;
            case UPSERT:
                nebulaConf.setMode(WriteMode.UPSERT);
                break;
            default:
                throw new UnsupportedTypeException("unsupport write mode: " + mode);
        }

        return nebulaConf;
    }

    private List<HostAddress> parseAddress(ReadableConfig config, ConfigOption<String> var1) {
        String strageServer = config.get(var1);
        String[] delimiters = strageServer.split(DELIMITERS);
        List<HostAddress> hostAddresses = new ArrayList<>();
        for (String delimiter : delimiters) {
            String[] hostAndPort = delimiter.split(SUB_DELIMITERS);
            if (hostAndPort.length != 2) {
                throw new IllegalArgumentException("address input format must be host:port!");
            }
            hostAddresses.add(new HostAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1])));
        }
        return hostAddresses;
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Stream.of(
                        SCHEMA_NAME,
                        SPACE,
                        SCHEMA_TYPE,
                        STORAGE_ADDRESSES,
                        GRAPHD_ADDRESSES,
                        PASSWORD,
                        USERNAME)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Stream.of(
                        CLIENT_CONNECT_TIMEOUT_OPTION,
                        IS_NEBULA_RECONECT,
                        SCHEMA_NAME,
                        FETCH_SIZE,
                        BULK_SIZE,
                        SPACE,
                        SCHEMA_TYPE,
                        ENABLE_SSL,
                        SSL_PARAM_TYPE,
                        CACRT_FILE_PATH,
                        CRT_FILE_PATH,
                        KEY_FILE_PATH,
                        SSL_PASSWORD,
                        CONNECTION_RETRY,
                        EXECUTION_RETRY,
                        STORAGE_ADDRESSES,
                        GRAPHD_ADDRESSES,
                        READ_TASKS,
                        WRITE_TASKS,
                        FETCH_INTERVAL,
                        START_TIME,
                        END_TIME,
                        DEFAULT_ALLOW_PART_SUCCESS,
                        DEFAULT_ALLOW_READ_FOLLOWER,
                        PASSWORD,
                        USERNAME,
                        MIN_CONNS_SIZE,
                        MAX_CONNS_SIZE,
                        IDLE_TIME,
                        INTERVAL_IDLE,
                        WAIT_IDLE_TIME,
                        WRITE_MODE)
                .collect(Collectors.toSet());
    }

    protected NebulaInputFormatBuilder getInputFormatBuilder() {
        return new NebulaInputFormatBuilder(new NebulaInputFormat());
    }

    protected NebulaOutputFormatBuilder getOutputFormatBuilder() {
        return new NebulaOutputFormatBuilder(new NebulaOutputFormat());
    }
}
