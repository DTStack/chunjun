package com.dtstack.flinkx.connector.influxdb.sink;

import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.connector.influxdb.conf.InfluxdbConfig;
import com.dtstack.flinkx.connector.influxdb.converter.InfluxdbRawTypeConverter;
import com.dtstack.flinkx.converter.RawTypeConverter;
import com.dtstack.flinkx.sink.SinkFactory;
import com.dtstack.flinkx.util.JsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;

/** @Author xirang @Company Dtstack @Date: 2022/3/14 9:56 AM */
public class InfluxdbSinkFactory extends SinkFactory {

    protected InfluxdbConfig influxdbConfig;

    public InfluxdbSinkFactory(SyncConf syncConf) {
        super(syncConf);
        influxdbConfig =
                JsonUtil.toObject(
                        JsonUtil.toJson(syncConf.getWriter().getParameter()), InfluxdbConfig.class);
        influxdbConfig.setColumn(syncConf.getWriter().getFieldList());
        super.initFlinkxCommonConf(influxdbConfig);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return InfluxdbRawTypeConverter::apply;
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        InfluxdbOutputFormatBuilder builder = new InfluxdbOutputFormatBuilder();
        builder.setInfluxdbConfig(influxdbConfig);
        builder.setColumnMetaInfos(influxdbConfig.getColumn());
        builder.setDatabase(influxdbConfig.getDatabase());
        builder.setMeasurement(influxdbConfig.getMeasurement());
        //        new InfluxdbColumnConverter()
        return createOutput(dataSet, builder.finish());
    }
}
