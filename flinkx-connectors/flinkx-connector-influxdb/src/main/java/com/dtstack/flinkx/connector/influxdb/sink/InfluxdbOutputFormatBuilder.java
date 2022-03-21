package com.dtstack.flinkx.connector.influxdb.sink;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.influxdb.conf.InfluxdbConfig;
import com.dtstack.flinkx.connector.influxdb.conf.InfluxdbSinkConfig;
import com.dtstack.flinkx.sink.format.BaseRichOutputFormatBuilder;

import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

/** @Author xirang @Company Dtstack @Date: 2022/3/14 2:57 PM */
public class InfluxdbOutputFormatBuilder extends BaseRichOutputFormatBuilder {

    private InfluxdbOutputFormat format;

    public InfluxdbOutputFormatBuilder() {
        super.format = this.format = new InfluxdbOutputFormat();
    }

    public void setInfluxdbConfig(InfluxdbConfig config) {
        super.setConfig(config);
        format.setConfig(config);
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

    public void setColumnMetaInfos(List<FieldConf> columnMetaInfos) {
        if (columnMetaInfos != null && !columnMetaInfos.isEmpty()) {
            List<String> names =
                    columnMetaInfos.stream().map(FieldConf::getName).collect(Collectors.toList());
            setColumnNames(names);
            List<String> values =
                    columnMetaInfos.stream().map(FieldConf::getType).collect(Collectors.toList());
            setColumnTypes(values);
        }
    }

    public void setColumnTypes(List<String> columnTypes) {
        format.setColumnTypes(columnTypes);
    }

    public void setColumnNames(List<String> columnNames) {
        format.setColumnNames(columnNames);
    }
}
