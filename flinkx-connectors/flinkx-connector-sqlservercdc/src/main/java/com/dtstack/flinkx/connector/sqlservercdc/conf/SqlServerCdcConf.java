package com.dtstack.flinkx.connector.sqlservercdc.conf;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.conf.FlinkxCommonConf;

import java.util.List;

public class SqlServerCdcConf extends FlinkxCommonConf {

    private String username;
    private String password;
    private String url;
    private String databaseName;
    private String cat;
    private boolean pavingData;
    private List<String> tableList;
    private Long pollInterval;
    private String lsn;
    private boolean splitUpdate;
    private String timestampFormat = "sql";
    private List<FieldConf> column;

    public String getTimestampFormat() {
        return timestampFormat;
    }

    public void setTimestampFormat(String timestampFormat) {
        this.timestampFormat = timestampFormat;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getCat() {
        return cat;
    }

    public void setCat(String cat) {
        this.cat = cat;
    }

    public boolean isPavingData() {
        return pavingData;
    }

    public void setPavingData(boolean pavingData) {
        this.pavingData = pavingData;
    }

    public List<String> getTableList() {
        return tableList;
    }

    public void setTableList(List<String> tableList) {
        this.tableList = tableList;
    }

    public Long getPollInterval() {
        return pollInterval;
    }

    public void setPollInterval(Long pollInterval) {
        this.pollInterval = pollInterval;
    }

    public String getLsn() {
        return lsn;
    }

    public void setLsn(String lsn) {
        this.lsn = lsn;
    }

    public boolean isSplitUpdate() {
        return splitUpdate;
    }

    public void setSplitUpdate(boolean splitUpdate) {
        this.splitUpdate = splitUpdate;
    }

    public List<FieldConf> getColumn() {
        return column;
    }

    public void setColumn(List<FieldConf> column) {
        this.column = column;
    }

    @Override
    public String toString() {
        return "SqlserverCdcConf{" +
                "username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", url='" + url + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", cat='" + cat + '\'' +
                ", pavingData=" + pavingData +
                ", tableList=" + tableList +
                ", pollInterval=" + pollInterval +
                ", lsn='" + lsn + '\'' +
                '}';
    }
}
