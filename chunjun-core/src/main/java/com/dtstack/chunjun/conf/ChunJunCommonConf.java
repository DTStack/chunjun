/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.chunjun.conf;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Date: 2021/04/08 Company: www.dtstack.com
 *
 * @author tudou
 */
public class ChunJunCommonConf implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 速率上限，0代表不限速 */
    private long speedBytes = 0;
    /** 容忍的最大脏数据条数 */
    private int errorRecord = 0;
    /** 容忍的最大脏数据比例，-1代表不校验比例 */
    private int errorPercentage = -1;
    /** 脏数据文件的绝对路径，支持本地和hdfs */
    private String dirtyDataPath;
    /** hdfs时，Hadoop的配置 */
    private Map<String, Object> dirtyDataHadoopConf;
    /** 脏数据对应的字段名称列表 */
    private List<String> fieldNameList = Collections.emptyList();
    /** 是否校验format */
    private boolean checkFormat = true;
    /** 并行度 */
    private Integer parallelism = 1;
    /** table field column conf */
    private List<FieldConf> column;
    /** Number of batches written */
    private int batchSize = 1;
    /** Time when the timer is regularly written to the database */
    private long flushIntervalMills = 10000L;
    /** whether to execute ddlRowdata */
    private boolean executeDdlAble;
    /** sp path */
    private String savePointPath;

    /** metrics plugin root */
    private String metricPluginRoot;

    /** metrics plugin name */
    private String metricPluginName;

    /** calculate rowData size */
    private String rowSizeCalculatorType;

    /** two phase mode */
    private String semantic = "at-least-once";

    /** metrics plugin properties */
    private Map<String, Object> metricProps;

    public String getSemantic() {
        return semantic;
    }

    public void setSemantic(String semantic) {
        this.semantic = semantic;
    }

    public String getMetricPluginRoot() {
        return metricPluginRoot;
    }

    public void setMetricPluginRoot(String metricPluginRoot) {
        this.metricPluginRoot = metricPluginRoot;
    }

    public String getMetricPluginName() {
        return metricPluginName == null ? "prometheus" : metricPluginName;
    }

    public void setMetricPluginName(String metricPluginName) {
        this.metricPluginName = metricPluginName;
    }

    public String getRowSizeCalculatorType() {
        return rowSizeCalculatorType == null ? "objectSizeCalculator" : rowSizeCalculatorType;
    }

    public void setRowSizeCalculatorType(String rowSizeCalculatorType) {
        this.rowSizeCalculatorType = rowSizeCalculatorType;
    }

    public Map<String, Object> getMetricProps() {
        return metricProps;
    }

    public void setMetricProps(Map<String, Object> metricProps) {
        this.metricProps = metricProps;
    }

    public long getSpeedBytes() {
        return speedBytes;
    }

    public void setSpeedBytes(long speedBytes) {
        this.speedBytes = speedBytes;
    }

    public int getErrorRecord() {
        return errorRecord;
    }

    public void setErrorRecord(int errorRecord) {
        this.errorRecord = errorRecord;
    }

    public int getErrorPercentage() {
        return errorPercentage;
    }

    public void setErrorPercentage(int errorPercentage) {
        this.errorPercentage = errorPercentage;
    }

    public String getDirtyDataPath() {
        return dirtyDataPath;
    }

    public void setDirtyDataPath(String dirtyDataPath) {
        this.dirtyDataPath = dirtyDataPath;
    }

    public Map<String, Object> getDirtyDataHadoopConf() {
        return dirtyDataHadoopConf;
    }

    public void setDirtyDataHadoopConf(Map<String, Object> dirtyDataHadoopConf) {
        this.dirtyDataHadoopConf = dirtyDataHadoopConf;
    }

    public List<String> getFieldNameList() {
        return fieldNameList;
    }

    public void setFieldNameList(List<String> fieldNameList) {
        this.fieldNameList = fieldNameList;
    }

    public boolean isCheckFormat() {
        return checkFormat;
    }

    public void setCheckFormat(boolean checkFormat) {
        this.checkFormat = checkFormat;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    public void setParallelism(Integer parallelism) {
        this.parallelism = parallelism;
    }

    public List<FieldConf> getColumn() {
        return column;
    }

    public void setColumn(List<FieldConf> column) {
        this.column = column;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public long getFlushIntervalMills() {
        return flushIntervalMills;
    }

    public void setFlushIntervalMills(long flushIntervalMills) {
        this.flushIntervalMills = flushIntervalMills;
    }

    public String getSavePointPath() {
        return savePointPath;
    }

    public void setSavePointPath(String savePointPath) {
        this.savePointPath = savePointPath;
    }

    public boolean isExecuteDdlAble() {
        return executeDdlAble;
    }

    public void setExecuteDdlAble(boolean executeDdlAble) {
        this.executeDdlAble = executeDdlAble;
    }

    @Override
    public String toString() {
        return "ChunJunCommonConf{"
                + "speedBytes="
                + speedBytes
                + ", errorRecord="
                + errorRecord
                + ", errorPercentage="
                + errorPercentage
                + ", dirtyDataPath='"
                + dirtyDataPath
                + '\''
                + ", dirtyDataHadoopConf="
                + dirtyDataHadoopConf
                + ", fieldNameList="
                + fieldNameList
                + ", checkFormat="
                + checkFormat
                + ", parallelism="
                + parallelism
                + ", column="
                + column
                + ", batchSize="
                + batchSize
                + ", executeDdlAble="
                + executeDdlAble
                + ", flushIntervalMills="
                + flushIntervalMills
                + ", metricPluginRoot='"
                + metricPluginRoot
                + '\''
                + ", metricPluginName='"
                + metricPluginName
                + '\''
                + ", metricProps="
                + metricProps
                + ", savePointPath="
                + savePointPath
                + '}';
    }
}
