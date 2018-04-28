package com.dtstack.flinkx.rdb.outputformat;

import com.dtstack.flinkx.rdb.DatabaseInterface;
import com.dtstack.flinkx.outputformat.RichOutputFormatBuilder;

import java.util.List;
import java.util.Map;

public class JdbcOutputFormatBuilder extends RichOutputFormatBuilder {

    private JdbcOutputFormat format;

    public JdbcOutputFormatBuilder() {
        super.format = format = new JdbcOutputFormat();
    }

    public void setUsername(String username) {
        format.username = username;
    }

    public void setPassword(String password) {
        format.password = password;
    }

    public void setDrivername(String drivername) {
        format.drivername = drivername;
    }

    public void setDBUrl(String dbURL) {
        format.dbURL = dbURL;
    }

    public void setPreSql(List<String> preSql) {
        format.preSql = preSql;
    }

    public void setPostSql(List<String> postSql) {
        format.postSql = postSql;
    }

    public void setUpdateKey(Map<String,List<String>> updateKey) {
        format.updateKey = updateKey;
    }

    public void setDatabaseInterface(DatabaseInterface databaseInterface) {
        format.databaseInterface = databaseInterface;
    }

    public void setMode(String mode) {
        format.mode = mode;
    }

    public void setTable(String table) {
        format.table = table;
    }

    public void setColumn(List<String> column) {
        format.column = column;
    }

    public void setFullColumn(List<String> fullColumn) {
        format.fullColumn = fullColumn;
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
        if (format.drivername == null) {
            throw new IllegalArgumentException("No driver supplied");
        }
    }

}
