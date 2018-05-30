package com.dtstack.flinkx.postgresql;

import com.dtstack.flinkx.rdb.BaseDatabaseMeta;

import java.util.List;

/**
 * @author jiangbo
 * @date 2018/5/25 11:26
 */
public class PostgresqlDatabaseMeta extends BaseDatabaseMeta {
    @Override
    protected String makeMultipleValues(int nCols, int batchSize) {
        return null;
    }

    @Override
    protected String makeValues(int nCols) {
        return null;
    }

    @Override
    protected String makeValues(List<String> column) {
        return null;
    }

    @Override
    public String getDatabaseType() {
        return "postgresql";
    }

    @Override
    public String getDriverClass() {
        return "org.postgresql.Driver";
    }

    @Override
    public String getSQLQueryFields(String tableName) {
        return String.format("SELECT * FROM %s LIMIT 0",tableName);
    }

    @Override
    public String getSQLQueryColumnFields(List<String> column, String table) {
        return "SELECT " + quoteColumns(column) + " FROM " + quoteTable(table) + " LIMIT 0";
    }

    @Override
    public String getSplitFilter(String columnName) {
        return null;
    }
}
