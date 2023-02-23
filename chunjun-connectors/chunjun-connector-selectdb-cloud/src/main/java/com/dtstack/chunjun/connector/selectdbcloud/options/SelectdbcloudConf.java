package com.dtstack.chunjun.connector.selectdbcloud.options;

import com.dtstack.chunjun.conf.ChunJunCommonConf;

import org.apache.flink.table.types.DataType;

import java.util.Properties;

public class SelectdbcloudConf extends ChunJunCommonConf {

    private String host;

    private String httpPort;

    private String queryPort;

    private String cluster;

    private String username;

    private String password;

    private String tableIdentifier;

    private Integer maxRetries;

    private Boolean enableDelete;

    private String[] fieldNames;

    private DataType[] fieldDataTypes;

    private Properties loadProperties;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getHttpPort() {
        return httpPort;
    }

    public void setHttpPort(String httpPort) {
        this.httpPort = httpPort;
    }

    public String getQueryPort() {
        return queryPort;
    }

    public void setQueryPort(String queryPort) {
        this.queryPort = queryPort;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
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

    public String getTableIdentifier() {
        return tableIdentifier;
    }

    public void setTableIdentifier(String tableIdentifier) {
        this.tableIdentifier = tableIdentifier;
    }

    public Integer getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(Integer maxRetries) {
        this.maxRetries = maxRetries;
    }

    public Boolean getEnableDelete() {
        return enableDelete;
    }

    public void setEnableDelete(Boolean enableDelete) {
        this.enableDelete = enableDelete;
    }

    public String[] getFieldNames() {
        return fieldNames;
    }

    public void setFieldNames(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }

    public DataType[] getFieldDataTypes() {
        return fieldDataTypes;
    }

    public void setFieldDataTypes(DataType[] fieldDataTypes) {
        this.fieldDataTypes = fieldDataTypes;
    }

    public Properties getLoadProperties() {
        return loadProperties;
    }

    public void setLoadProperties(Properties loadProperties) {
        this.loadProperties = loadProperties;
    }

    public String getHttpUrl() {
        return host + ":" + httpPort;
    }

    public String getQueryUrl() {
        return host + ":" + queryPort;
    }
}
