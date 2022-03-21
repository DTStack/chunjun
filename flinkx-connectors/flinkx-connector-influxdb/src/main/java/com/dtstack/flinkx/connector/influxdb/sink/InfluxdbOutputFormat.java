package com.dtstack.flinkx.connector.influxdb.sink;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.influxdb.conf.InfluxdbSinkConfig;
import com.dtstack.flinkx.connector.influxdb.converter.InfluxdbColumnConverter;
import com.dtstack.flinkx.connector.influxdb.converter.InfluxdbRawTypeConverter;
import com.dtstack.flinkx.connector.influxdb.enums.TimePrecisionEnums;
import com.dtstack.flinkx.sink.format.BaseRichOutputFormat;
import com.dtstack.flinkx.throwable.WriteRecordException;
import com.dtstack.flinkx.util.TableUtil;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.StringUtils;

import okhttp3.OkHttpClient;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** @Author xirang @Company Dtstack @Date: 2022/3/14 2:57 PM */
public class InfluxdbOutputFormat extends BaseRichOutputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxdbOutputFormat.class);

    private InfluxdbSinkConfig sinkConfig;

    private InfluxDB influxDB;

    private List<String> tags;

    private String timestamp;

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
        influxDB.writeWithRetry(batchPoints.build());
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        this.timestamp = sinkConfig.getTimestamp();
        this.precision = TimePrecisionEnums.of(sinkConfig.getPrecision()).getPrecision();
        this.tags = sinkConfig.getTags();
        establishConnnection();
        influxDB.setDatabase(database);
        List<FieldConf> column = sinkConfig.getColumn();
        columnNameList = column.stream().map(FieldConf::getName).collect(Collectors.toList());
        columnTypeList = column.stream().map(FieldConf::getType).collect(Collectors.toList());
        RowType rowType =
                TableUtil.createRowType(
                        columnNameList, columnTypeList, InfluxdbRawTypeConverter::apply);
        setRowConverter(
                new InfluxdbColumnConverter(
                        rowType,
                        sinkConfig,
                        measurement,
                        columnNameList,
                        tags,
                        timestamp,
                        precision));
    }

    @Override
    protected void closeInternal() throws IOException {
        if (enableBatch) influxDB.disableBatch();
        if (influxDB != null) Runtime.getRuntime().addShutdownHook(new Thread(influxDB::close));
    }

    private void establishConnnection() {
        if (influxDB != null) return;
        LOG.info("Get the connection for influxdb");
        OkHttpClient.Builder clientBuilder =
                new OkHttpClient.Builder()
                        .connectTimeout(15000, TimeUnit.MILLISECONDS)
                        .readTimeout(sinkConfig.getWriteTimeout(), TimeUnit.SECONDS);
        influxDB =
                InfluxDBFactory.connect(
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
            options.precision(precision);
            options.bufferLimit(sinkConfig.getBatchSize());
            options.flushDuration(sinkConfig.getFlushDuration());
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
