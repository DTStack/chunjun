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

package com.dtstack.chunjun.connector.nebula.table;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.nebula.config.NebulaConfig;
import com.dtstack.chunjun.connector.nebula.config.NebulaSSLParam;
import com.dtstack.chunjun.connector.nebula.sink.NebulaDynamicTableSink;
import com.dtstack.chunjun.connector.nebula.sink.NebulaOutputFormat;
import com.dtstack.chunjun.connector.nebula.sink.NebulaOutputFormatBuilder;
import com.dtstack.chunjun.connector.nebula.source.NebulaDynamicTableSource;
import com.dtstack.chunjun.connector.nebula.source.NebulaInputFormat;
import com.dtstack.chunjun.connector.nebula.source.NebulaInputFormatBuilder;
import com.dtstack.chunjun.connector.nebula.utils.NebulaSchemaFamily;
import com.dtstack.chunjun.lookup.config.LookupConfig;
import com.dtstack.chunjun.sink.WriteMode;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import com.vesoft.nebula.client.graph.data.HostAddress;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.BULK_SIZE;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.CACRT_FILE_PATH;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.CLIENT_CONNECT_TIMEOUT_OPTION;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.CONNECTION_RETRY;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.CRT_FILE_PATH;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.DEFAULT_ALLOW_PART_SUCCESS;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.DEFAULT_ALLOW_READ_FOLLOWER;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.ENABLE_SSL;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.END_TIME;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.EXECUTION_RETRY;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.FETCH_INTERVAL;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.FETCH_SIZE;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.FIX_STRING_LEN;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.GRAPHD_ADDRESSES;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.IDLE_TIME;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.INTERVAL_IDLE;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.IS_NEBULA_RECONECT;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.KEY_FILE_PATH;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.MAX_CONNS_SIZE;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.MIN_CONNS_SIZE;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.PASSWORD;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.READ_TASKS;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.SCHEMA_NAME;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.SCHEMA_TYPE;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.SPACE;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.SSL_PARAM_TYPE;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.SSL_PASSWORD;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.START_TIME;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.STORAGE_ADDRESSES;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.USERNAME;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.VID_TYPE;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.WAIT_IDLE_TIME;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.WRITE_MODE;
import static com.dtstack.chunjun.connector.nebula.config.NebulaOptions.WRITE_TASKS;
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

public class NebulaDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    /** 通过该值查找具体插件 */
    private static final String IDENTIFIER = "nebula-x";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        Tuple3<ResolvedSchema, NebulaConfig, ReadableConfig> tuple3 = commonParse(context);
        String[] fieldNames = tuple3.f0.getColumnNames().toArray(new String[0]);
        List<FieldConfig> columnList = new ArrayList<>(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            FieldConfig field = new FieldConfig();
            field.setName(fieldNames[i]);
            field.setType(
                    TypeConfig.fromString(
                            (InternalTypeInfo.of(tuple3.f0.toPhysicalRowDataType().getLogicalType())
                                            .toRowType())
                                    .getTypeAt(i)
                                    .asSummaryString()));
            field.setIndex(i);
            columnList.add(field);
        }
        tuple3.f1.setFields(columnList);
        return new NebulaDynamicTableSink(tuple3.f0, tuple3.f1, getOutputFormatBuilder());
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        Tuple3<ResolvedSchema, NebulaConfig, ReadableConfig> tuple3 = commonParse(context);
        return new NebulaDynamicTableSource(
                tuple3.f1,
                getLookupConf(tuple3.f2, context.getObjectIdentifier().getObjectName()),
                tuple3.f0,
                getInputFormatBuilder());
    }

    private Tuple3<ResolvedSchema, NebulaConfig, ReadableConfig> commonParse(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        String[] fieldNames = resolvedSchema.getColumnNames().toArray(new String[0]);
        NebulaConfig nebulaConfig = parseNebulaConfig(config);
        nebulaConfig.setColumnNames(Arrays.asList(fieldNames));
        return new Tuple3<>(resolvedSchema, nebulaConfig, config);
    }

    private LookupConfig getLookupConf(ReadableConfig config, String tableName) {
        return LookupConfig.build()
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

    private NebulaConfig parseNebulaConfig(ReadableConfig config) {
        NebulaConfig nebulaConfig = new NebulaConfig();
        nebulaConfig.setStart(config.get(START_TIME));
        nebulaConfig.setEnd(config.get(END_TIME));
        nebulaConfig.setInterval(config.get(FETCH_INTERVAL));
        nebulaConfig.setReadTasks(config.get(READ_TASKS));
        nebulaConfig.setWriteTasks(config.get(WRITE_TASKS));
        nebulaConfig.setDefaultAllowPartSuccess(config.get(DEFAULT_ALLOW_PART_SUCCESS));
        nebulaConfig.setDefaultAllowReadFollower(config.get(DEFAULT_ALLOW_READ_FOLLOWER));
        nebulaConfig.setFetchSize(config.get(FETCH_SIZE));
        nebulaConfig.setCaCrtFilePath(config.get(CACRT_FILE_PATH));
        nebulaConfig.setCrtFilePath(config.get(CRT_FILE_PATH));
        nebulaConfig.setEnableSSL(config.get(ENABLE_SSL));
        nebulaConfig.setConnectionRetry(config.get(CONNECTION_RETRY));
        nebulaConfig.setEntityName(config.get(SCHEMA_NAME));
        nebulaConfig.setExecutionRetry(config.get(EXECUTION_RETRY));
        nebulaConfig.setKeyFilePath(config.get(KEY_FILE_PATH));
        nebulaConfig.setPassword(config.get(PASSWORD));
        nebulaConfig.setReconn(config.get(IS_NEBULA_RECONECT));
        String s = config.get(SCHEMA_TYPE);
        switch (s) {
            case TAG:
            case VERTEX:
                nebulaConfig.setSchemaType(NebulaSchemaFamily.VERTEX);
                break;
            case EDGE:
            case EDGE_TYPE:
                nebulaConfig.setSchemaType(NebulaSchemaFamily.EDGE);
                break;
            default:
                throw new UnsupportedTypeException("unsupported nebula schema type!");
        }
        nebulaConfig.setStorageAddresses(parseAddress(config, STORAGE_ADDRESSES));
        nebulaConfig.setGraphdAddresses(parseAddress(config, GRAPHD_ADDRESSES));
        nebulaConfig.setSpace(config.get(SPACE));
        nebulaConfig.setVidType(config.get(VID_TYPE));

        String sslParamType = config.get(SSL_PARAM_TYPE);
        if (StringUtils.isNotBlank(sslParamType)) {
            switch (sslParamType) {
                case SELF:
                    nebulaConfig.setSslParamType(NebulaSSLParam.SELF_SIGNED_SSL_PARAM);
                    break;
                case CAS:
                    nebulaConfig.setSslParamType(NebulaSSLParam.CA_SIGNED_SSL_PARAM);
                    break;
                default:
                    throw new UnsupportedTypeException("unsupport ssl param Type!");
            }
        }
        nebulaConfig.setSslPassword(config.get(SSL_PASSWORD));
        nebulaConfig.setTimeout(config.get(CLIENT_CONNECT_TIMEOUT_OPTION));
        nebulaConfig.setUsername(config.get(USERNAME));
        nebulaConfig.setMaxConnsSize(config.get(MAX_CONNS_SIZE));
        nebulaConfig.setMinConnsSize(config.get(MIN_CONNS_SIZE));
        nebulaConfig.setIdleTime(config.get(IDLE_TIME));
        nebulaConfig.setIntervalIdle(config.get(INTERVAL_IDLE));
        nebulaConfig.setWaitTime(config.get(WAIT_IDLE_TIME));
        nebulaConfig.setStringLength(config.get(FIX_STRING_LEN));
        nebulaConfig.setBatchSize(config.get(BULK_SIZE));
        String mode = config.get(WRITE_MODE);
        switch (mode) {
            case INSERT:
                nebulaConfig.setMode(WriteMode.INSERT);
                break;
            case UPSERT:
                nebulaConfig.setMode(WriteMode.UPSERT);
                break;
            default:
                throw new UnsupportedTypeException("unsupport write mode: " + mode);
        }

        return nebulaConfig;
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
