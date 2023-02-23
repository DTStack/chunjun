package com.dtstack.chunjun.connector.selectdbcloud.sink;

import com.dtstack.chunjun.connector.selectdbcloud.options.SelectdbcloudConf;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormatBuilder;

public class SelectdbcloudOutputFormatBuilder
        extends BaseRichOutputFormatBuilder<SelectdbcloudOutputFormat> {

    public SelectdbcloudOutputFormatBuilder(SelectdbcloudOutputFormat format) {
        super(format);
    }

    public SelectdbcloudOutputFormatBuilder setConf(SelectdbcloudConf conf) {

        super.setConfig(conf);
        format.setConf(conf);
        return this;
    }

    @Override
    protected void checkFormat() {}
}
