package com.dtstack.flinkx.binlog.reader;


import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * config of binlog
 *
 * @author jiangbo @ 2020/1/10
 */
public class BinlogConfig implements Serializable {

    public String host;

    public int port = 3306;

    public String username;

    public String password;

    public String jdbcUrl;

    public Map<String, Object> start;

    public String cat;

    public String filter;

    public long period = 1000L;

    public int bufferSize = 1024;

    public boolean pavingData = true;

    public List<String> table;

    public long slaveId = new Object().hashCode();

    private String connectionCharset = "UTF-8";

    private boolean detectingEnable = true;

    @JsonProperty("detectingSQL")
    private String detectingSql = "SELECT CURRENT_DATE";

    private boolean enableTsdb = true;

    private boolean parallel = true;

    private int parallelThreadSize = 2;

    @JsonProperty("isGTIDMode")
    private boolean isGtidMode = false;

    public boolean getGtidMode() {
        return isGtidMode;
    }

    public void setGtidMode(boolean gtidMode) {
        isGtidMode = gtidMode;
    }

    public int getParallelThreadSize() {
        return parallelThreadSize;
    }

    public void setParallelThreadSize(int parallelThreadSize) {
        this.parallelThreadSize = parallelThreadSize;
    }

    public boolean getParallel() {
        return parallel;
    }

    public void setParallel(boolean parallel) {
        this.parallel = parallel;
    }

    public boolean getEnableTsdb() {
        return enableTsdb;
    }

    public void setEnableTsdb(boolean enableTsdb) {
        this.enableTsdb = enableTsdb;
    }

    public String getDetectingSql() {
        return detectingSql;
    }

    public void setDetectingSql(String detectingSql) {
        this.detectingSql = detectingSql;
    }

    public boolean getDetectingEnable() {
        return detectingEnable;
    }

    public void setDetectingEnable(boolean detectingEnable) {
        this.detectingEnable = detectingEnable;
    }

    public String getConnectionCharset() {
        return connectionCharset;
    }

    public void setConnectionCharset(String connectionCharset) {
        this.connectionCharset = connectionCharset;
    }

    public long getSlaveId() {
        return slaveId;
    }

    public void setSlaveId(long slaveId) {
        this.slaveId = slaveId;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
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

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public Map<String, Object> getStart() {
        return start;
    }

    public void setStart(Map<String, Object> start) {
        this.start = start;
    }

    public String getCat() {
        return cat;
    }

    public void setCat(String cat) {
        this.cat = cat;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public long getPeriod() {
        return period;
    }

    public void setPeriod(long period) {
        this.period = period;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public boolean getPavingData() {
        return pavingData;
    }

    public void setPavingData(boolean pavingData) {
        this.pavingData = pavingData;
    }

    public List<String> getTable() {
        return table;
    }

    public void setTable(List<String> table) {
        this.table = table;
    }

    @Override
    public String toString() {
        return "BinlogConfig{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", username='" + username + '\'' +
                ", password='" + "******" + '\'' +
                ", jdbcUrl='" + jdbcUrl + '\'' +
                ", start=" + start +
                ", cat='" + cat + '\'' +
                ", filter='" + filter + '\'' +
                ", period=" + period +
                ", bufferSize=" + bufferSize +
                ", pavingData=" + pavingData +
                ", table=" + table +
                ", slaveId=" + slaveId +
                ", connectionCharset='" + connectionCharset + '\'' +
                ", detectingEnable=" + detectingEnable +
                ", detectingSQL='" + detectingSql + '\'' +
                ", enableTsdb=" + enableTsdb +
                ", parallel=" + parallel +
                ", parallelThreadSize=" + parallelThreadSize +
                ", isGTIDMode=" + isGtidMode +
                '}';
    }
}
