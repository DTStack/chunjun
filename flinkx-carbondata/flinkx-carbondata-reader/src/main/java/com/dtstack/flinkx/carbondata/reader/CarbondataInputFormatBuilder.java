package com.dtstack.flinkx.carbondata.reader;

import com.dtstack.flinkx.inputformat.RichInputFormatBuilder;
import org.apache.flink.util.Preconditions;
import java.util.List;
import java.util.Map;

public class CarbondataInputFormatBuilder extends RichInputFormatBuilder {

    private CarbondataInputFormat format;

    public CarbondataInputFormatBuilder() {
        super.format = format = new CarbondataInputFormat();
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

    public void setColumnNames(List<String> columnNames) {
        format.columnName = columnNames;
    }

    public void setColumnValues(List<String> columnValues) {
        format.columnValue = columnValues;
    }

    public void setColumnTypes(List<String> columnTypes) {
        format.columnType = columnTypes;
    }

    public void setFilter(String filter) {
        format.filter = filter;
    }


    @Override
    protected void checkFormat() {
        Preconditions.checkNotNull(format.hadoopConfig);
        Preconditions.checkNotNull(format.table);
        Preconditions.checkNotNull(format.path);
        Preconditions.checkNotNull(format.database);
        Preconditions.checkNotNull(format.columnName);
        Preconditions.checkNotNull(format.columnType);
        Preconditions.checkNotNull(format.columnValue);
        Preconditions.checkArgument(format.columnName.size() == format.columnType.size());
        Preconditions.checkArgument(format.columnName.size() == format.columnValue.size());
    }

}
