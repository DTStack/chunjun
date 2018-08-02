package com.dtstack.flinkx.rdb.inputformat;

import com.dtstack.flinkx.inputformat.RichInputFormatBuilder;
import com.dtstack.flinkx.rdb.DataSource;
import com.dtstack.flinkx.rdb.DatabaseInterface;
import com.dtstack.flinkx.rdb.type.TypeConverterInterface;

import java.util.List;

public class DistributedJdbcInputFormatBuilder extends RichInputFormatBuilder {

    private DistributedJdbcInputFormat format;

    public DistributedJdbcInputFormatBuilder() {
        super.format = this.format = new DistributedJdbcInputFormat();
    }

    public void setDrivername(String driverName) {
        format.driverName = driverName;
    }

    public void setUsername(String username) {
        format.username = username;
    }

    public void setPassword(String password) {
        format.password = password;
    }

    public void setDatabaseInterface(DatabaseInterface databaseInterface) {
        format.databaseInterface = databaseInterface;
    }

    public void setTypeConverter(TypeConverterInterface converter){
        format.typeConverter = converter;
    }

    public void setColumn(List<String> column){
        format.column = column;
    }

    public void setSplitKey(String splitKey){
        format.splitKey = splitKey;
    }

    public void setSourceList(List<DataSource> sourceList){
        format.sourceList = sourceList;
    }

    public void setNumPartitions(int numPartitions){
        format.numPartitions = numPartitions;
    }

    @Override
    protected void checkFormat() {
        
    }
}
