/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.dtstack.flinkx.carbondata.writer.dict;


import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnIdentifier;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import java.util.List;


/**
 * Dictionary Load Model
 *
 * Company: www.dtstack.com
 * @author huyifan_zju@163.com
 */
public class DictionaryLoadModel {

    AbsoluteTableIdentifier table;

    CarbonDimension[] dimensions;

    String hdfsLocation;

    String dictfolderPath;

    String[] dictFilePaths;

    Boolean[] dictFileExists;

    List<Boolean> isComplexes;

    List<CarbonDimension> primDimensions;

    String[] delimiters;

    ColumnIdentifier[] columnIdentifier;

    boolean isFirstLoad;

    String hdfsTempLocation;

    String lockType;

    String zooKeeperUrl;

    String serializationNullFormat;

    String defaultTimestampFormat;

    String defaultDateFormat;

    public DictionaryLoadModel(
            AbsoluteTableIdentifier table,
            CarbonDimension[] dimensions,
            String hdfsLocation,
            String dictfolderPath,
            String[] dictFilePaths,
            Boolean[] dictFileExists,
            List<Boolean> isComplexes,
            List<CarbonDimension> primDimensions,
            String[] delimiters,
            ColumnIdentifier[] columnIdentifier,
            boolean isFirstLoad,
            String hdfsTempLocation,
            String lockType,
            String zooKeeperUrl,
            String serializationNullFormat,
            String defaultTimestampFormat,
            String defaultDateFormat) {
        this.table = table;
        this.dimensions = dimensions;
        this.hdfsLocation = hdfsLocation;
        this.dictfolderPath = dictfolderPath;
        this.dictFilePaths = dictFilePaths;
        this.dictFileExists = dictFileExists;
        this.isComplexes = isComplexes;
        this.primDimensions = primDimensions;
        this.delimiters = delimiters;
        this.columnIdentifier = columnIdentifier;
        this.isFirstLoad = isFirstLoad;
        this.hdfsTempLocation = hdfsTempLocation;
        this.lockType = lockType;
        this.zooKeeperUrl = zooKeeperUrl;
        this.serializationNullFormat = serializationNullFormat;
        this.defaultTimestampFormat = defaultTimestampFormat;
        this.defaultDateFormat = defaultDateFormat;
    }

    public AbsoluteTableIdentifier getTable() {
        return table;
    }

    public void setTable(AbsoluteTableIdentifier table) {
        this.table = table;
    }

    public CarbonDimension[] getDimensions() {
        return dimensions;
    }

    public void setDimensions(CarbonDimension[] dimensions) {
        this.dimensions = dimensions;
    }

    public String getHdfsLocation() {
        return hdfsLocation;
    }

    public void setHdfsLocation(String hdfsLocation) {
        this.hdfsLocation = hdfsLocation;
    }

    public String getDictfolderPath() {
        return dictfolderPath;
    }

    public void setDictfolderPath(String dictfolderPath) {
        this.dictfolderPath = dictfolderPath;
    }

    public String[] getDictFilePaths() {
        return dictFilePaths;
    }

    public void setDictFilePaths(String[] dictFilePaths) {
        this.dictFilePaths = dictFilePaths;
    }

    public List<Boolean> getIsComplexes() {
        return isComplexes;
    }

    public void setIsComplexes(List<Boolean> isComplexes) {
        this.isComplexes = isComplexes;
    }

    public List<CarbonDimension> getPrimDimensions() {
        return primDimensions;
    }

    public void setPrimDimensions(List<CarbonDimension> primDimensions) {
        this.primDimensions = primDimensions;
    }

    public Boolean[] getDictFileExists() {
        return dictFileExists;
    }

    public void setDictFileExists(Boolean[] dictFileExists) {
        this.dictFileExists = dictFileExists;
    }

    public ColumnIdentifier[] getColumnIdentifier() {
        return columnIdentifier;
    }

    public void setColumnIdentifier(ColumnIdentifier[] columnIdentifier) {
        this.columnIdentifier = columnIdentifier;
    }

    public String[] getDelimiters() {
        return delimiters;
    }

    public void setDelimiters(String[] delimiters) {
        this.delimiters = delimiters;
    }

    public boolean isFirstLoad() {
        return isFirstLoad;
    }

    public void setFirstLoad(boolean firstLoad) {
        isFirstLoad = firstLoad;
    }

    public String getHdfsTempLocation() {
        return hdfsTempLocation;
    }

    public void setHdfsTempLocation(String hdfsTempLocation) {
        this.hdfsTempLocation = hdfsTempLocation;
    }

    public String getLockType() {
        return lockType;
    }

    public void setLockType(String lockType) {
        this.lockType = lockType;
    }

    public String getZooKeeperUrl() {
        return zooKeeperUrl;
    }

    public void setZooKeeperUrl(String zooKeeperUrl) {
        this.zooKeeperUrl = zooKeeperUrl;
    }

    public String getSerializationNullFormat() {
        return serializationNullFormat;
    }

    public void setSerializationNullFormat(String serializationNullFormat) {
        this.serializationNullFormat = serializationNullFormat;
    }

    public String getDefaultTimestampFormat() {
        return defaultTimestampFormat;
    }

    public void setDefaultTimestampFormat(String defaultTimestampFormat) {
        this.defaultTimestampFormat = defaultTimestampFormat;
    }

    public String getDefaultDateFormat() {
        return defaultDateFormat;
    }

    public void setDefaultDateFormat(String defaultDateFormat) {
        this.defaultDateFormat = defaultDateFormat;
    }
}
