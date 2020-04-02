package com.dtstack.flinkx.cassandra.writer;

import com.dtstack.flinkx.outputformat.BaseRichOutputFormatBuilder;
import com.dtstack.flinkx.reader.MetaColumn;

import java.util.List;
import java.util.Map;

/**
 *
 * @Company: www.dtstack.com
 * @author wuhui
 */
public class CassandraOutputFormatBuilder extends BaseRichOutputFormatBuilder {

    private CassandraOutputFormat format;

    public CassandraOutputFormatBuilder() {
        super.format = format = new CassandraOutputFormat();
    }

    public void setBatchSize(Long batchSize) {
        format.batchSize = batchSize;
    }

    public void setColumn(List<MetaColumn> column) {
        format.columnMeta = column;
    }

    public void setAsyncWrite(boolean asyncWrite) {
        format.asyncWrite = asyncWrite;
    }

    public void setKeySpace(String keySpace) {
        format.keySpace = keySpace;
    }

    public void setTable(String table) {
        format.table = table;
    }

    public void setConsistancyLevel(String consistancyLevel) {
        format.consistancyLevel = consistancyLevel;
    }

    public void setCassandraConfig(Map<String,Object> cassandraConfig){
        format.cassandraConfig = cassandraConfig;
    }

    @Override
    protected void checkFormat() {
        if (format.getRestoreConfig() != null && format.getRestoreConfig().isRestore()){
            throw new UnsupportedOperationException("This plugin not support restore from failed state");
        }
    }
}
