package com.dtstack.flinkx.rdb;

import java.io.Serializable;

public class DataSource implements Serializable {

    private String jdbcUrl;
    private String userName;
    private String password;
    private String table;
    private boolean isSplitByKey;
    private Object[] parameterValues;
    private boolean isFinished;

    public DataSource() {
        this.isSplitByKey = false;
        this.isFinished = false;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public boolean isSplitByKey() {
        return isSplitByKey;
    }

    public void setSplitByKey(boolean splitByKey) {
        isSplitByKey = splitByKey;
    }

    public Object[] getParameterValues() {
        return parameterValues;
    }

    public void setParameterValues(Object[] parameterValues) {
        this.parameterValues = parameterValues;
    }

    public boolean isFinished() {
        return isFinished;
    }

    public void setFinished(boolean finished) {
        isFinished = finished;
    }
}
