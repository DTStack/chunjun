package com.dtstack.chunjun.connector.iceberg.source;

import com.dtstack.chunjun.connector.iceberg.conf.IcebergReaderConf;
import com.dtstack.chunjun.source.format.BaseRichInputFormatBuilder;

import org.apache.commons.collections.CollectionUtils;
import org.apache.iceberg.flink.source.FlinkInputFormat;

public class IcebergInputFormatBuilder extends BaseRichInputFormatBuilder<IcebergInputFormat> {

    public IcebergInputFormatBuilder() {
        super(new IcebergInputFormat());
    }

    public void setIcebergConf(IcebergReaderConf icebergConf) {
        super.setConfig(icebergConf);
        format.setIcebergReaderConf(icebergConf);
    }

    public void setInput(FlinkInputFormat input) {
        format.setInput(input);
    }

    @Override
    protected void checkFormat() {
        if (CollectionUtils.isEmpty(format.getConfig().getColumn())) {
            throw new IllegalArgumentException("columns can not be empty");
        }
    }
}
