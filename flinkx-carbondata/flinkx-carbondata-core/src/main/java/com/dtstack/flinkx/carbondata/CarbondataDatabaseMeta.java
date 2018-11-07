package com.dtstack.flinkx.carbondata;


import com.dtstack.flinkx.rdb.BaseDatabaseMeta;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;


/**
 * The class of Carbondata prototype
 *
 * Company: www.dtstack.com
 * @author huyifan_zju@163.com
 */
public class CarbondataDatabaseMeta extends BaseDatabaseMeta {

    @Override
    public String getStartQuote() {
        return "`";
    }

    @Override
    public String getEndQuote() {
        return "`";
    }

    @Override
    protected String makeMultipleValues(int nCols, int batchSize) {
        return null;
    }

    @Override
    protected String makeValues(int nCols) {
        return "(" + StringUtils.repeat("?", ",", nCols) + ")";
    }

    @Override
    protected String makeValues(List<String> column) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getDatabaseType() {
        return "carbondata";
    }

    @Override
    public String getDriverClass() {
        return "org.apache.hive.jdbc.HiveDriver";
    }

    @Override
    public String getSQLQueryFields(String tableName) {
        return "SELECT * FROM " + tableName + " LIMIT 0";
    }

    @Override
    public String getSQLQueryColumnFields(List<String> column, String table) {
        return "SELECT " + quoteColumns(column) + " FROM " + quoteTable(table) + " LIMIT 0";
    }

    @Override
    public String getSplitFilter(String columnName) {
        return String.format("pmod(%s, ${N}) = ${M}", getStartQuote() + columnName + getEndQuote());
    }


}
