package com.dtstack.flinkx.carbondata.writer;

import com.dtstack.flinkx.outputformat.RichOutputFormatBuilder;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Map;

/**
 * The Builder class of CarbondataOutputFormat
 *
 * Company: www.dtstack.com
 * @author huyifan_zju@163.com
 */
public class CarbondataOutputFormatBuilder extends RichOutputFormatBuilder {

    private CarbonOutputFormat format;

    public CarbondataOutputFormatBuilder() {
        super.format = format = new CarbonOutputFormat();
    }

    public void setHadoopConfig(Map<String,String> hadoopConfig) {
        format.hadoopConfig = hadoopConfig;
    }

    public void setTable(String table) {
        format.table = table;
    }

    public void setPath(String path) {
        format.path = path;
    }

    public void setDatabase(String database) {
        format.database = database;
    }

    public void setColumn(List<String> column) {
        format.column = column;
    }

    @Override
    protected void checkFormat() {
        Preconditions.checkNotNull(format.hadoopConfig);
        Preconditions.checkNotNull(format.table);
        Preconditions.checkNotNull(format.path);
        Preconditions.checkNotNull(format.database);
        Preconditions.checkNotNull(format.column);

    }
}
