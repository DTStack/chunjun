package com.dtstack.flinkx.solr.writrer;

import com.dtstack.flinkx.outputformat.BaseRichOutputFormatBuilder;
import com.dtstack.flinkx.reader.MetaColumn;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class SolrOutputFormatBuilder extends BaseRichOutputFormatBuilder {

    private SolrOutputFormat format;

    public SolrOutputFormatBuilder() {
        super.format = format = new SolrOutputFormat();
    }

    public void setAddress(String address){
        format.address = address;
    }

    public void setColumns(List<MetaColumn> metaColumnList){
        format.metaColumns = metaColumnList;
    }


    @Override
    protected void checkFormat() {
        if (format.getRestoreConfig() != null && format.getRestoreConfig().isRestore()){
            throw new UnsupportedOperationException("This plugin not support restore from failed state");
        }
        if (StringUtils.isBlank(format.address)) {
            throw new IllegalArgumentException("solr address is must");
        }

    }
}
