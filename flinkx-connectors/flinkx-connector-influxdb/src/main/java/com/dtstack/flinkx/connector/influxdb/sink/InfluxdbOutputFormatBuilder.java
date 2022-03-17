package com.dtstack.flinkx.connector.influxdb.sink;

import com.dtstack.flinkx.connector.influxdb.conf.InfluxdbConfig;
import com.dtstack.flinkx.connector.influxdb.conf.InfluxdbSinkConfig;
import com.dtstack.flinkx.sink.format.BaseRichOutputFormatBuilder;

import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.StringUtils;

/** @Author xirang @Company Dtstack @Date: 2022/3/14 2:57 PM */
public class InfluxdbOutputFormatBuilder extends BaseRichOutputFormatBuilder {

    private InfluxdbOutputFormat format;

    public InfluxdbOutputFormatBuilder() {
        super.format = this.format = new InfluxdbOutputFormat();
    }

    public void setInfluxdbConfig(InfluxdbConfig config) {
        super.setConfig(config);
        this.format.setConfig(config);
    }

    @Override
    protected void checkFormat() {
        InfluxdbSinkConfig sinkConfig = format.getSinkConfig();
        StringBuilder sb = new StringBuilder(256);
        if (CollectionUtil.isNullOrEmpty(sinkConfig.getUrl())) sb.append("No url supplied;\n");
        if (StringUtils.isNullOrWhitespaceOnly(sinkConfig.getUsername()))
            sb.append("No username supplied;\n");
        if (StringUtils.isNullOrWhitespaceOnly(sinkConfig.getPassword()))
            sb.append("No password supplied;\n");
        if (sb.length() > 0) {
            throw new IllegalArgumentException(sb.toString());
        }
    }
}
