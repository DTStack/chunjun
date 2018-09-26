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

    public void setWhere(String where){
        format.where = where;
    }

    @Override
    protected void checkFormat() {

        boolean hasGlobalCountInfo = true;
        if(format.username == null || format.password == null){
            hasGlobalCountInfo = false;
        }

        if (format.sourceList == null || format.sourceList.size() == 0){
            throw new IllegalArgumentException("One or more data sources must be specified");
        }

        String jdbcPrefix = null;

        for (DataSource dataSource : format.sourceList) {
            if(!hasGlobalCountInfo && (dataSource.getUserName() == null || dataSource.getPassword() == null)){
                throw new IllegalArgumentException("Must specify a global account or specify an account for each data source");
            }

            if (dataSource.getTable() == null || dataSource.getTable().length() == 0){
                throw new IllegalArgumentException("table name cannot be empty");
            }

            if (dataSource.getJdbcUrl() == null || dataSource.getJdbcUrl().length() == 0 ){
                throw new IllegalArgumentException("'jdbcUrl' cannot be empty");
            }

            if(jdbcPrefix == null){
                jdbcPrefix = dataSource.getJdbcUrl().split("//")[0];
            }

            if(!dataSource.getJdbcUrl().startsWith(jdbcPrefix)){
                throw new IllegalArgumentException("Multiple data sources must be of the same type");
            }
        }
    }
}
