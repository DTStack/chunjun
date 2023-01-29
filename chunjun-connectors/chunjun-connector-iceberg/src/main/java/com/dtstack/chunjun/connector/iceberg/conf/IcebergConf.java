package com.dtstack.chunjun.connector.iceberg.conf;

import com.dtstack.chunjun.conf.ChunJunCommonConf;
import com.dtstack.chunjun.sink.WriteMode;

import java.util.HashMap;
import java.util.Map;

public class IcebergConf extends ChunJunCommonConf {
    private String warehouse;

    private String uri;

    private Map<String, Object> hadoopConfig = new HashMap<>();

    private String database;

    private String table;

    private String writeMode = WriteMode.APPEND.name();

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getDatabase() {
        return database;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getTable() {
        return table;
    }

    public void setWarehouse(String warehouse) {
        this.warehouse = warehouse;
    }

    public String getWarehouse() {
        return warehouse;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getUri() {
        return uri;
    }

    public void setHadoopConfig(Map<String, Object> hadoopConfig) {
        this.hadoopConfig = hadoopConfig;
    }

    public Map<String, Object> getHadoopConfig() {
        return hadoopConfig;
    }

    public String getWriteMode() {
        return writeMode;
    }

    public void setWriteMode(String writeMode) {
        this.writeMode = writeMode;
    }
}
