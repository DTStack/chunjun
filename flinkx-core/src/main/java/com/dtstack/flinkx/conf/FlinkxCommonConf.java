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
package com.dtstack.flinkx.conf;

import com.dtstack.flinkx.constants.ConfigConstant;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Date: 2021/04/08
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class FlinkxCommonConf implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 速率上限，0代表不限速 */
    private long bytes = 0;
    /** 容忍的最大脏数据条数 */
    private int record = 0;
    /** 容忍的最大脏数据比例，-1代表不校验比例 */
    private int percentage = -1;
    /** 是否开启FlinkX日志记录，默认不开启 */
    private boolean isLogger = false;
    /** 日志记录的日志级别 */
    private String logLevel = "info";
    /** 本地日志保存路径，路径不存在会自动创建 */
    private String logPath = "/tmp/dtstack/flinkx/";
    /** 日志格式，默认为log4j格式 */
    private String logPattern = ConfigConstant.DEFAULT_LOG4J_PATTERN;
    /** 脏数据文件的绝对路径，支持本地和hdfs */
    private String dirtyDataPath;
    /** hdfs时，Hadoop的配置 */
    private Map<String, Object> dirtyDataHadoopConf;
    /** 脏数据对应的字段名称列表 */
    private List<String> fieldNameList = Collections.emptyList();
    /** 是否校验format */
    private boolean checkFormat = true;

    public long getBytes() {
        return bytes;
    }

    public void setBytes(long bytes) {
        this.bytes = bytes;
    }

    public int getRecord() {
        return record;
    }

    public void setRecord(int record) {
        this.record = record;
    }

    public int getPercentage() {
        return percentage;
    }

    public void setPercentage(int percentage) {
        this.percentage = percentage;
    }

    public boolean isLogger() {
        return isLogger;
    }

    public void setLogger(boolean logger) {
        isLogger = logger;
    }

    public String getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

    public String getLogPath() {
        return logPath;
    }

    public void setLogPath(String logPath) {
        this.logPath = logPath;
    }

    public String getLogPattern() {
        return logPattern;
    }

    public void setLogPattern(String logPattern) {
        this.logPattern = logPattern;
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

    @Override
    public String toString() {
        return "FlinkxCommonConf{" +
                "bytes=" + bytes +
                ", record=" + record +
                ", percentage=" + percentage +
                ", isLogger=" + isLogger +
                ", logLevel='" + logLevel + '\'' +
                ", logPath='" + logPath + '\'' +
                ", logPattern='" + logPattern + '\'' +
                ", dirtyDataPath='" + dirtyDataPath + '\'' +
                ", dirtyDataHadoopConf=" + dirtyDataHadoopConf +
                ", fieldNameList=" + fieldNameList +
                ", checkFormat=" + checkFormat +
                '}';
    }
}
