/*
 *
 *  *
 *  *  * Licensed to the Apache Software Foundation (ASF) under one
 *  *  * or more contributor license agreements.  See the NOTICE file
 *  *  * distributed with this work for additional information
 *  *  * regarding copyright ownership.  The ASF licenses this file
 *  *  * to you under the Apache License, Version 2.0 (the
 *  *  * "License"); you may not use this file except in compliance
 *  *  * with the License.  You may obtain a copy of the License at
 *  *  *
 *  *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *  *
 *  *  * Unless required by applicable law or agreed to in writing, software
 *  *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  * See the License for the specific language governing permissions and
 *  *  * limitations under the License.
 *  *
 *
 */

package com.dtstack.flinkx.connector.influxdb.source;

import com.dtstack.flinkx.connector.influxdb.conf.InfluxdbSourceConfig;
import com.dtstack.flinkx.connector.influxdb.converter.InfluxdbColumnConverter;
import com.dtstack.flinkx.connector.influxdb.converter.InfluxdbRawTypeConverter;
import com.dtstack.flinkx.source.format.BaseRichInputFormat;
import com.dtstack.flinkx.throwable.ReadRecordException;
import com.dtstack.flinkx.util.ColumnBuildUtil;
import com.dtstack.flinkx.util.TableUtil;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import okhttp3.HttpUrl;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.impl.InfluxDBImpl;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static com.dtstack.flinkx.connector.influxdb.constants.InfluxdbCons.QUERY_FIELD;
import static com.dtstack.flinkx.connector.influxdb.constants.InfluxdbCons.QUERY_TAG;

/**
 * Company：www.dtstack.com.
 *
 * @author shitou
 * @date 2022/3/8
 */
public class InfluxdbInputFormat extends BaseRichInputFormat {

    private InfluxdbSourceConfig config;
    private String queryTemplate;
    private transient InfluxDB influxDB;
    private transient AtomicBoolean hasNext;
    private transient BlockingQueue<Map<String, Object>> queue;
    private transient InfluxdbQuerySqlBuilder queryInfluxQLBuilder;

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        InfluxdbInputSplit[] splits = new InfluxdbInputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            splits[i] = new InfluxdbInputSplit(i, minNumSplits, i);
        }
        return splits;
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        LOG.info("subTask[{}] inputSplit = {}.", indexOfSubTask, inputSplit);
        this.queue = new LinkedBlockingQueue<>(config.getFetchSize() * 3);
        this.hasNext = new AtomicBoolean(true);
        connect();

        Pair<List<String>, List<String>> pair = getTableMetadata();
        Pair<List<String>, List<String>> columnPair =
                ColumnBuildUtil.handleColumnList(
                        config.getColumn(), pair.getLeft(), pair.getRight());
        columnNameList = columnPair.getLeft();
        columnTypeList = columnPair.getRight();
        RowType rowType =
                TableUtil.createRowType(
                        columnNameList, columnTypeList, InfluxdbRawTypeConverter::apply);

        // TODO add InfluxdbRawConverter
        setRowConverter(
                new InfluxdbColumnConverter(rowType, config, columnNameList, config.getFormat()));

        this.queryInfluxQLBuilder = new InfluxdbQuerySqlBuilder(config, columnNameList);
        this.queryTemplate = queryInfluxQLBuilder.buildSql();
        String querySql = buildQuerySql(inputSplit);
        LOG.info("subTask[{}] querySql = {}.", indexOfSubTask, querySql);

        this.influxDB.query(
                new Query(querySql, config.getDatabase()),
                config.getFetchSize(),
                getConsumer(),
                () -> {
                    LOG.debug("subTask[{}] reader influxDB data is over.", indexOfSubTask);
                    hasNext.set(false);
                },
                throwable -> {
                    hasNext.set(false);
                    throwable.printStackTrace();
                });
    }

    @Override
    protected RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        RowData row;
        try {
            Map<String, Object> data = queue.poll(5, TimeUnit.SECONDS);
            row = rowConverter.toInternal(data);
        } catch (Exception e) {
            throw new ReadRecordException("can not read next record.", e, -1, rowData);
        }
        return row;
    }

    @Override
    protected void closeInternal() throws IOException {
        if (influxDB != null) {
            influxDB.close();
            influxDB = null;
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !hasNext.get() && queue.isEmpty();
    }

    private String buildQuerySql(InputSplit inputSplit) {
        String querySql = queryTemplate;

        if (inputSplit == null) {
            LOG.warn("inputSplit = null, Executing sql is: '{}'", querySql);
            return querySql;
        }

        InfluxdbInputSplit influxDBInputSplit = (InfluxdbInputSplit) inputSplit;

        if (StringUtils.isNotBlank(config.getSplitPk())) {
            querySql =
                    queryTemplate
                            .replace(
                                    "${N}",
                                    String.valueOf(influxDBInputSplit.getTotalNumberOfSplits()))
                            .replace("${M}", String.valueOf(influxDBInputSplit.getMod()));
        }
        return querySql;
    }

    public void connect() throws ConnectException {
        if (influxDB == null) {
            OkHttpClient.Builder clientBuilder =
                    new OkHttpClient.Builder()
                            .connectTimeout(15000, TimeUnit.MILLISECONDS)
                            .readTimeout(config.getQueryTimeOut(), TimeUnit.SECONDS);
            InfluxDB.ResponseFormat format = InfluxDB.ResponseFormat.valueOf(config.getFormat());
            clientBuilder.addInterceptor(
                    new Interceptor() {
                        @NotNull
                        @Override
                        public Response intercept(@NotNull Chain chain) throws IOException {
                            Request request = chain.request();
                            HttpUrl httpUrl =
                                    request.url()
                                            .newBuilder()
                                            // add common parameter
                                            .addQueryParameter("epoch", config.getEpoch())
                                            .build();
                            Request build = request.newBuilder().url(httpUrl).build();
                            Response response = chain.proceed(build);
                            return response;
                        }
                    });
            influxDB =
                    new InfluxDBImpl(
                            config.getUrl().get(0),
                            StringUtils.isEmpty(config.getUsername()) ? null : config.getUsername(),
                            StringUtils.isEmpty(config.getPassword()) ? null : config.getPassword(),
                            clientBuilder,
                            format);
            String version = influxDB.version();
            if (!influxDB.ping().isGood()) {
                String errorMessage =
                        String.format(
                                "connect influxdb failed, due to influxdb version info is unknown, the url is: {%s}",
                                config.getUrl().get(0));
                throw new ConnectException(errorMessage);
            }
            LOG.info("connect influxdb successful. sever version :{}.", version);
        }
    }

    public void setConfig(InfluxdbSourceConfig config) {
        this.config = config;
    }

    /**
     * get all measurement keys, include tags and fields.
     *
     * @return fields list
     */
    private Pair<List<String>, List<String>> getTableMetadata() {
        List<String> columnNames = new ArrayList<>();
        List<String> columnTypes = new ArrayList<>();
        QueryResult queryResult =
                influxDB.query(
                        new Query(
                                QUERY_FIELD.replace("${measurement}", config.getMeasurement()),
                                config.getDatabase()));
        List<QueryResult.Series> serieList = queryResult.getResults().get(0).getSeries();
        if (!CollectionUtils.isEmpty(serieList)) {
            for (List<Object> value : serieList.get(0).getValues()) {
                columnNames.add(String.valueOf(value.get(0)));
                columnTypes.add(String.valueOf(value.get(1)));
            }
        }

        // Check if spillPk is compliant
        if (config.getParallelism() > 1) {
            judgeSplitPkCompliant(columnNames, columnTypes, config.getSplitPk());
        }

        queryResult =
                influxDB.query(
                        new Query(
                                QUERY_TAG.replace("${measurement}", config.getMeasurement()),
                                config.getDatabase()));
        serieList = queryResult.getResults().get(0).getSeries();
        if (!CollectionUtils.isEmpty(serieList)) {
            for (List<Object> value : serieList.get(0).getValues()) {
                columnNames.add(String.valueOf(value.get(0)));
                // Tag keys and tag values are both strings.
                columnTypes.add("string");
            }
        }
        // add time field.
        columnNames.add("time");
        columnTypes.add("long");
        return Pair.of(columnNames, columnTypes);
    }

    /**
     * get the consumer to invoke for each received QueryResult.
     *
     * @return consumer
     */
    private BiConsumer<InfluxDB.Cancellable, QueryResult> getConsumer() {
        return (cancellable, queryResult) -> {
            try {
                if (CollectionUtils.isEmpty(queryResult.getResults())
                        || "DONE".equalsIgnoreCase(queryResult.getError())) {
                    LOG.info("results is empty and this query is done.");
                } else {
                    for (QueryResult.Result result : queryResult.getResults()) {
                        List<QueryResult.Series> serieList = result.getSeries();
                        if (CollectionUtils.isNotEmpty(serieList)) {
                            for (QueryResult.Series series : serieList) {
                                List<String> columnList = series.getColumns();
                                for (List<Object> values : series.getValues()) {
                                    Map<String, Object> data = new HashMap<>();
                                    for (int i = 0; i < columnList.size(); i++) {
                                        data.put(columnList.get(i), values.get(i));
                                    }
                                    queue.put(data);
                                }
                            }
                        } else {
                            // 没有数据
                            LOG.debug(
                                    "subTask[{}] reader influxDB series is empty.", indexOfSubTask);
                            hasNext.set(false);
                        }
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        };
    }

    /**
     * Check if spiltPk complies with the requirements, 1.splitPk is a field key; 2. splitPk is
     * integer.
     *
     * @param columnNames field keys
     * @param columnTypes field types
     * @param splitPk splitPk
     */
    private void judgeSplitPkCompliant(
            List<String> columnNames, List<String> columnTypes, String splitPk) {
        Optional<String> key =
                columnNames.stream().filter(name -> StringUtils.equals(splitPk, name)).findFirst();
        if (key.isPresent()) {
            int index = columnNames.indexOf(key.get());
            if (!StringUtils.equalsIgnoreCase("integer", columnTypes.get(index))) {
                String errorMessage =
                        "spiltPk must be of type integer, but is actually "
                                + columnTypes.get(index);
                throw new IllegalArgumentException(errorMessage);
            }

        } else {
            String errorMessage =
                    "splitPk must be field, field keys is: "
                            + columnNames
                            + ", splitPk is: "
                            + splitPk;
            throw new IllegalArgumentException(errorMessage);
        }
    }
}
