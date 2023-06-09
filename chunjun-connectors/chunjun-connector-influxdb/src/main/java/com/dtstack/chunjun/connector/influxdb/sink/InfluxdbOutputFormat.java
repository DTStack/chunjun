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

package com.dtstack.chunjun.connector.influxdb.sink;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.influxdb.config.InfluxdbSinkConfig;
import com.dtstack.chunjun.connector.influxdb.converter.InfluxdbRawTypeMapper;
import com.dtstack.chunjun.connector.influxdb.converter.InfluxdbSyncConverter;
import com.dtstack.chunjun.connector.influxdb.enums.TimePrecisionEnums;
import com.dtstack.chunjun.constants.Metrics;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormat;
import com.dtstack.chunjun.throwable.WriteRecordException;
import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.StringUtils;

import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.impl.InfluxDBImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class InfluxdbOutputFormat extends BaseRichOutputFormat {

    private static final long serialVersionUID = -3677021871315224915L;

    private InfluxdbSinkConfig sinkConfig;

    private InfluxDB influxDB;

    private String database;

    private String measurement;

    private TimeUnit precision;

    private boolean enableBatch;

    @Override
    protected void writeSingleRecordInternal(RowData rowData) throws WriteRecordException {
        try {
            Point.Builder builder = Point.measurement(measurement);
            rowConverter.toExternal(rowData, builder);
            influxDB.write(builder.build());
        } catch (Exception e) {
            throw new WriteRecordException("Writer data to influxdb error", e, 0, rowData);
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        BatchPoints.Builder batchPoints = BatchPoints.builder();
        List<Point> pointList = new ArrayList<>();
        for (RowData row : rows) {
            Point.Builder builder = Point.measurement(measurement);
            rowConverter.toExternal(row, builder);
            pointList.add(builder.build());
        }
        batchPoints.points(pointList);
        influxDB.write(batchPoints.build());
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        String timestamp = sinkConfig.getTimestamp();
        this.precision = TimePrecisionEnums.of(sinkConfig.getPrecision()).getPrecision();
        List<String> tags = sinkConfig.getTags();
        establishConnection();
        influxDB.setDatabase(database);
        List<FieldConfig> column = sinkConfig.getColumn();
        columnNameList = column.stream().map(FieldConfig::getName).collect(Collectors.toList());
        columnTypeList = column.stream().map(FieldConfig::getType).collect(Collectors.toList());
        RowType rowType =
                TableUtil.createRowType(
                        columnNameList, columnTypeList, InfluxdbRawTypeMapper::apply);
        setRowConverter(
                new InfluxdbSyncConverter(
                        rowType, sinkConfig, columnNameList, tags, timestamp, precision));
    }

    @Override
    protected void closeInternal() {
        if (enableBatch) influxDB.disableBatch();
        if (influxDB != null) Runtime.getRuntime().addShutdownHook(new Thread(influxDB::close));
    }

    private void establishConnection() {
        if (influxDB != null) return;
        log.info("Get the connection for influxdb");
        OkHttpClient.Builder clientBuilder =
                new OkHttpClient.Builder()
                        .connectTimeout(15000, TimeUnit.MILLISECONDS)
                        .readTimeout(sinkConfig.getWriteTimeout(), TimeUnit.SECONDS);
        influxDB =
                new InfluxDBImpl(
                        sinkConfig.getUrl().get(0),
                        StringUtils.isNullOrWhitespaceOnly(sinkConfig.getUsername())
                                ? null
                                : sinkConfig.getUsername(),
                        StringUtils.isNullOrWhitespaceOnly(sinkConfig.getPassword())
                                ? null
                                : sinkConfig.getPassword(),
                        clientBuilder);
        String rp = sinkConfig.getRp();
        if (!StringUtils.isNullOrWhitespaceOnly(rp)) influxDB.setRetentionPolicy(rp);
        if (enableBatch) {
            BatchOptions options = BatchOptions.DEFAULTS;
            options =
                    options.exceptionHandler(
                                    (iterable, e) -> {
                                        for (Point point : iterable) {
                                            long globalErrors =
                                                    accumulatorCollector.getAccumulatorValue(
                                                            Metrics.NUM_ERRORS, false);
                                            dirtyManager.collect(point, e, null, globalErrors);
                                        }
                                        if (log.isTraceEnabled()) {
                                            log.trace(
                                                    "write data error, e = {}",
                                                    ExceptionUtil.getErrorMessage(e));
                                        }
                                    })
                            .precision(precision)
                            .bufferLimit(sinkConfig.getBufferLimit())
                            .flushDuration(sinkConfig.getFlushDuration());
            influxDB.enableBatch(options);
        }
    }

    public void setConfig(InfluxdbSinkConfig config) {
        this.sinkConfig = config;
    }

    public InfluxdbSinkConfig getSinkConfig() {
        return sinkConfig;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public void setMeasurement(String measurement) {
        this.measurement = measurement;
    }

    public void setEnableBatch(boolean enableBatch) {
        this.enableBatch = enableBatch;
    }
}
