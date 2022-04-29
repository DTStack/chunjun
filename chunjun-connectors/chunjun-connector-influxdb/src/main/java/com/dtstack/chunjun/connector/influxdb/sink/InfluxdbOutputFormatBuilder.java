package com.dtstack.chunjun.connector.influxdb.sink;

import com.dtstack.chunjun.connector.influxdb.conf.InfluxdbSinkConfig;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormatBuilder;

import org.apache.flink.util.CollectionUtil;

/** @Author xirang @Company Dtstack @Date: 2022/3/14 2:57 PM */
public class InfluxdbOutputFormatBuilder extends BaseRichOutputFormatBuilder {

    private InfluxdbOutputFormat format;

    public InfluxdbOutputFormatBuilder() {
        super.format = this.format = new InfluxdbOutputFormat();
    }

    public void setInfluxdbConfig(InfluxdbSinkConfig config) {
        super.setConfig(config);
        format.setConfig(config);
    }

    @Override
    protected void checkFormat() {
        InfluxdbSinkConfig sinkConfig = format.getSinkConfig();
        StringBuilder sb = new StringBuilder(256);
        if (CollectionUtil.isNullOrEmpty(sinkConfig.getUrl())) sb.append("No url supplied;\n");
        if (!"insert".equalsIgnoreCase(sinkConfig.getWriteMode().getMode()))
            sb.append("Only support insert write mode;\n");
        if (sb.length() > 0) {
            throw new IllegalArgumentException(sb.toString());
        }
    }

    public void setDatabase(String database) {
        format.setDatabase(database);
    }

    public void setMeasurement(String measurement) {
        format.setMeasurement(measurement);
    }

    public void setEnableBatch(boolean enableBatch) {
        format.setEnableBatch(enableBatch);
    }
}
