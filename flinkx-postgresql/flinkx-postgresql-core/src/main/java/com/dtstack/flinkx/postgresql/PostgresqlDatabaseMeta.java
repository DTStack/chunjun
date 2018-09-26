package com.dtstack.flinkx.postgresql;

import com.dtstack.flinkx.rdb.BaseDatabaseMeta;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author jiangbo
 * @date 2018/5/25 11:26
 */
public class PostgresqlDatabaseMeta extends BaseDatabaseMeta {

    @Override
    protected String makeMultipleValues(int nCols, int batchSize) {
        String value = makeValues(nCols);
        return StringUtils.repeat(value, ",", batchSize);
    }

    @Override
    protected String makeValues(int nCols) {
        return "(" + StringUtils.repeat("?", ",", nCols) + ")";
    }

    @Override
    protected String makeValues(List<String> column) {
        throw new UnsupportedOperationException();
    }

    private String makeUpdatePart (List<String> column) {
        List<String> updateList = new ArrayList<>();
        for(String col : column) {
            String quotedCol = quoteColumn(col);
            updateList.add(quotedCol + "=EXCLUDED." + quotedCol);
        }
        return StringUtils.join(updateList, ",");
    }

    private String makeUpdateKey(Map<String,List<String>> updateKey){
        Iterator<Map.Entry<String,List<String>>> it = updateKey.entrySet().iterator();
        return StringUtils.join(it.next().getValue(),",");
    }

    @Override
    public String getReplaceStatement(List<String> column, List<String> fullColumn, String table, Map<String,List<String>> updateKey) {
        return "INSERT INTO " + quoteTable(table)
                + " (" + quoteColumns(column) + ") VALUES "
                + makeValues(column.size())
                + "ON CONFLICT(" + makeUpdateKey(updateKey)
                + ") DO UPDATE SET " + makeUpdatePart(column) ;
    }

    @Override
    public String getMultiReplaceStatement(List<String> column, List<String> fullColumn, String table, int batchSize, Map<String,List<String>> updateKey) {
        return "INSERT INTO " + quoteTable(table)
                + " (" + quoteColumns(column) + ") VALUES "
                + makeMultipleValues(column.size(), batchSize)
                + "ON CONFLICT(" + makeUpdateKey(updateKey)
                + ") DO UPDATE SET " + makeUpdatePart(column) ;
    }

    @Override
    public String getStartQuote() {
        return "";
    }

    @Override
    public String getEndQuote() {
        return "";
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
        String sql = "select attrelid ::regclass as table_name, attname as col_name, atttypid ::regtype as col_type from pg_attribute \n" +
                "where attrelid = '%s' ::regclass and attnum > 0 and attisdropped = 'f'";
        return String.format(sql,table);
    }

    @Override
    public String getSplitFilter(String columnName) {
        return String.format(" mod(%s,?) = ?", getStartQuote() + columnName + getEndQuote());
    }

    @Override
    public int getFetchSize(){
        return 1000;
    }

    @Override
    public int getQueryTimeout(){
        return 1000;
    }
}
