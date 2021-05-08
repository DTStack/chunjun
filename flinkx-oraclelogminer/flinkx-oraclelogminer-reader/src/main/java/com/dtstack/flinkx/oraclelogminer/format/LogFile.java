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
package com.dtstack.flinkx.oraclelogminer.format;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * @author jiangbo
 * @date 2020/3/31
 */
public class LogFile {

    private String fileName;

    private BigDecimal firstChange;

    private BigDecimal nextChange;

    private Long thread;

    /** 日志文件状态 https://docs.oracle.com/cd/B12037_01/server.101/b10755/dynviews_1132.htm  V$LOGMNR_LOGS里的status */
    private int status;

    //是归档日志 还是online日志
    private String type;

    /** 文件大小  **/
    private Long bytes;

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public BigDecimal getFirstChange() {
        return firstChange;
    }

    public void setFirstChange(BigDecimal firstChange) {
        this.firstChange = firstChange;
    }

    public BigDecimal getNextChange() {
        return nextChange;
    }

    public void setNextChange(BigDecimal nextChange) {
        this.nextChange = nextChange;
    }

    public long getThread() {
        return thread;
    }

    public void setThread(Long thread) {
        this.thread = thread;
    }

    public Long getBytes() {
        return bytes;
    }

    public void setBytes(Long bytes) {
        this.bytes = bytes;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "LogFile{" +
                "fileName='" + fileName + '\'' +
                ", firstChange=" + firstChange +
                ", nextChange=" + nextChange +
                ", thread=" + thread +
                ", bytes=" + bytes +
                ", type=" + type +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o){
            return true;
        }

        if (o == null || getClass() != o.getClass()){
            return false;
        }

        LogFile logFile = (LogFile) o;
        return Objects.equals(fileName, logFile.fileName) &&
                Objects.equals(firstChange, logFile.firstChange) &&
                Objects.equals(thread, logFile.thread) &&
                Objects.equals(nextChange, logFile.nextChange);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileName, firstChange, nextChange, thread, bytes);
    }
}
