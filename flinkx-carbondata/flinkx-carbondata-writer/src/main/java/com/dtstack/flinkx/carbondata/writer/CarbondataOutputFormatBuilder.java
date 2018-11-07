package com.dtstack.flinkx.carbondata.writer;

import com.dtstack.flinkx.outputformat.RichOutputFormatBuilder;
import java.util.List;

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

    public void setUsername(String username) {
        format.username = username;
    }

    public void setPassword(String password) {
        format.password = password;
    }

    public void setDBUrl(String dbURL) {
        format.dbURL = dbURL;
    }

    public void setTable(String table) {
        format.table = table;
    }

    public void setColumn(List<String> column) {
        format.column = column;
    }

    public void setPreSql(List<String> preSql) {
        format.preSql = preSql;
    }

    public void setPostSql(List<String> postSql) {
        format.postSql = postSql;
    }

    @Override
    protected void checkFormat() {
        if (format.username == null) {
            LOG.info("Username was not supplied separately.");
        }
        if (format.password == null) {
            LOG.info("Password was not supplied separately.");
        }
        if (format.dbURL == null) {
            throw new IllegalArgumentException("No dababase URL supplied.");
        }
    }
}
