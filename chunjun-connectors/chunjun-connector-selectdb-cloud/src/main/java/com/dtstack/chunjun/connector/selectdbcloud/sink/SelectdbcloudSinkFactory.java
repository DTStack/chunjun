package com.dtstack.chunjun.connector.selectdbcloud.sink;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.selectdbcloud.common.LoadConstants;
import com.dtstack.chunjun.connector.selectdbcloud.converter.SelectdbcloudRowTypeConverter;
import com.dtstack.chunjun.connector.selectdbcloud.options.SelectdbcloudConf;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.util.JsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

public class SelectdbcloudSinkFactory extends SinkFactory {
    private final SelectdbcloudConf conf;

    public SelectdbcloudSinkFactory(SyncConf syncConf) {
        super(syncConf);
        conf =
                JsonUtil.toObject(
                        JsonUtil.toJson(syncConf.getWriter().getParameter()),
                        SelectdbcloudConf.class);
        conf.setFieldNames(
                conf.getColumn().stream().map(FieldConf::getName).toArray(String[]::new));
        conf.setFieldDataTypes(
                conf.getColumn().stream()
                        .map(t -> SelectdbcloudRowTypeConverter.apply(t.getType().toUpperCase()))
                        .toArray(DataType[]::new));
        setDefaults(conf);
        super.initCommonConf(conf);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return SelectdbcloudRowTypeConverter::apply;
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        SelectdbcloudOutputFormatBuilder builder =
                new SelectdbcloudOutputFormatBuilder(new SelectdbcloudOutputFormat()).setConf(conf);
        return createOutput(dataSet, builder.finish());
    }

    private void setDefaults(SelectdbcloudConf conf) {

        if (conf.getMaxRetries() == null) {
            conf.setMaxRetries(LoadConstants.DEFAULT_MAX_RETRY_TIMES);
        }

        if (conf.getBatchSize() < 1) {
            conf.setBatchSize(LoadConstants.DEFAULT_BATCH_SIZE);
        }

        if (conf.getFlushIntervalMills() < 1L) {
            conf.setFlushIntervalMills(LoadConstants.DEFAULT_INTERVAL_MILLIS);
        }

        if (conf.getEnableDelete() == null) {
            conf.setEnableDelete(LoadConstants.DEFAULT_ENABLE_DELETE);
        }
    }
}
