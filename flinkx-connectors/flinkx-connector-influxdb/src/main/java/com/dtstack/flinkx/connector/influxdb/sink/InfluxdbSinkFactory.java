package com.dtstack.flinkx.connector.influxdb.sink;

import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.converter.RawTypeConverter;
import com.dtstack.flinkx.sink.SinkFactory;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;

/**
 * @Author xirang
 * @Company Dtstack
 * @Date: 2022/3/14 9:56 AM
 */
public class InfluxdbSinkFactory extends SinkFactory {

    public InfluxdbSinkFactory(SyncConf syncConf) {
        super(syncConf);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return null;
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        return null;
    }
}
