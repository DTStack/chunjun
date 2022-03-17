package com.dtstack.flinkx.connector.influxdb.sink;

import com.dtstack.flinkx.connector.influxdb.conf.InfluxdbSinkConfig;
import com.dtstack.flinkx.connector.influxdb.enums.TimePrecisionEnums;
import com.dtstack.flinkx.sink.format.BaseRichOutputFormat;
import com.dtstack.flinkx.throwable.WriteRecordException;

import org.apache.flink.table.data.RowData;
import org.apache.flink.util.StringUtils;

import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** @Author xirang @Company Dtstack @Date: 2022/3/14 2:57 PM */
public class InfluxdbOutputFormat extends BaseRichOutputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxdbOutputFormat.class);

    private InfluxdbSinkConfig sinkConfig;

    private InfluxDB influxDB;

    private List<String> tags;

    private String timestamp;

    private TimeUnit precision;

    @Override
    protected void writeSingleRecordInternal(RowData rowData) throws WriteRecordException {}

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {}

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        this.timestamp = sinkConfig.getTimestamp();
        this.precision = TimePrecisionEnums.of(sinkConfig.getPrecision()).getPrecision();
        establishConnnection();
        this.tags = sinkConfig.getTags();
    }

    @Override
    protected void closeInternal() throws IOException {}

    private void establishConnnection() {
        if (influxDB != null) return;
        LOG.info("Get the connection for influxdb");
        influxDB =
                InfluxDBFactory.connect(
                        sinkConfig.getUrl().get(0),
                        sinkConfig.getUsername(),
                        sinkConfig.getPassword());
        String rp = sinkConfig.getRp();
        if (!StringUtils.isNullOrWhitespaceOnly(rp)) influxDB.setRetentionPolicy(rp);
        BatchOptions options = BatchOptions.DEFAULTS;
        options.precision(precision);
        options.bufferLimit(sinkConfig.getBatchSize());
        influxDB.enableBatch(options);
    }

    public InfluxdbSinkConfig getSinkConfig() {
        return sinkConfig;
    }

    public void setSinkConfig(InfluxdbSinkConfig sinkConfig) {
        this.sinkConfig = sinkConfig;
    }
}
