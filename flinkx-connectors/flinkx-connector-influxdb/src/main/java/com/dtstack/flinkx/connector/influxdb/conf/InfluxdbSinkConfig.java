package com.dtstack.flinkx.connector.influxdb.conf;

import com.dtstack.flinkx.sink.WriteMode;

import java.util.List;

/** @Author xirang @Company Dtstack @Date: 2022/3/14 6:00 PM */
public class InfluxdbSinkConfig extends InfluxdbConfig {

    /** retention policy for influxdb writer */
    private String rp;

    /** write mode for influxdb writer */
    private WriteMode writeMode = WriteMode.INSERT;

    /** tags of the measurement */
    private List<String> tags;

    private int batchSize = 10000;

    /** flush duration (ms) */
    private int flushDuration = 1000;

    private boolean enableBatch = true;

    public boolean isEnableBatch() {
        return enableBatch;
    }

    public void setEnableBatch(boolean enableBatch) {
        this.enableBatch = enableBatch;
    }

    public int getFlushDuration() {
        return flushDuration;
    }

    public void setFlushDuration(int flushDuration) {
        this.flushDuration = flushDuration;
    }

    /** the name of timestamp field */
    private String timestamp;

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    private int writeTimeout = 5;

    public int getWriteTimeout() {
        return writeTimeout;
    }

    public void setWriteTimeout(int writeTimeout) {
        this.writeTimeout = writeTimeout;
    }

    /** precision of Unix time */
    private String precision = "ns";

    public String getRp() {
        return rp;
    }

    public void setRp(String rp) {
        this.rp = rp;
    }

    public WriteMode getWriteMode() {
        return writeMode;
    }

    public void setWriteMode(WriteMode writeMode) {
        this.writeMode = writeMode;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    @Override
    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public String getPrecision() {
        return precision;
    }

    public void setPrecision(String precision) {
        this.precision = precision;
    }
}
