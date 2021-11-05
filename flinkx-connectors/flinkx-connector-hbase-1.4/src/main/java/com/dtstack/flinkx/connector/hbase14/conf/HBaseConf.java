/*
 *    Copyright 2021 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.dtstack.flinkx.connector.hbase14.conf;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.conf.FlinkxCommonConf;

import java.util.List;
import java.util.Map;

public class HBaseConf extends FlinkxCommonConf {

    private List<FieldConf> columnMetaInfos;
    private String encoding;
    private Map<String, Object> hbaseConfig;

    // reader
    private String startRowkey;
    private String endRowkey;
    private boolean isBinaryRowkey;
    private String table;
    private int scanCacheSize = 1000;

    // writer
    private String nullMode;
    private Boolean walFlag;
    private long writeBufferSize;
    private String rowkeyExpress;
    private Integer versionColumnIndex;
    private String versionColumnValue;

    public List<FieldConf> getColumnMetaInfos() {
        return columnMetaInfos;
    }

    public void setColumnMetaInfos(List<FieldConf> columnMetaInfos) {
        this.columnMetaInfos = columnMetaInfos;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public Map<String, Object> getHbaseConfig() {
        return hbaseConfig;
    }

    public void setHbaseConfig(Map<String, Object> hbaseConfig) {
        this.hbaseConfig = hbaseConfig;
    }

    public String getStartRowkey() {
        return startRowkey;
    }

    public void setStartRowkey(String startRowkey) {
        this.startRowkey = startRowkey;
    }

    public String getEndRowkey() {
        return endRowkey;
    }

    public void setEndRowkey(String endRowkey) {
        this.endRowkey = endRowkey;
    }

    public boolean isBinaryRowkey() {
        return isBinaryRowkey;
    }

    public void setBinaryRowkey(boolean binaryRowkey) {
        isBinaryRowkey = binaryRowkey;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String tableName) {
        this.table = table;
    }

    public int getScanCacheSize() {
        return scanCacheSize;
    }

    public void setScanCacheSize(int scanCacheSize) {
        this.scanCacheSize = scanCacheSize;
    }

    public String getNullMode() {
        return nullMode;
    }

    public void setNullMode(String nullMode) {
        this.nullMode = nullMode;
    }

    public Boolean getWalFlag() {
        return walFlag;
    }

    public void setWalFlag(Boolean walFlag) {
        this.walFlag = walFlag;
    }

    public long getWriteBufferSize() {
        return writeBufferSize;
    }

    public void setWriteBufferSize(long writeBufferSize) {
        this.writeBufferSize = writeBufferSize;
    }

    public String getRowkeyExpress() {
        return rowkeyExpress;
    }

    public void setRowkeyExpress(String rowkeyExpress) {
        this.rowkeyExpress = rowkeyExpress;
    }

    public Integer getVersionColumnIndex() {
        return versionColumnIndex;
    }

    public void setVersionColumnIndex(Integer versionColumnIndex) {
        this.versionColumnIndex = versionColumnIndex;
    }

    public String getVersionColumnValue() {
        return versionColumnValue;
    }

    public void setVersionColumnValue(String versionColumnValue) {
        this.versionColumnValue = versionColumnValue;
    }
}
