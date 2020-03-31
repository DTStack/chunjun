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

/**
 * @author jiangbo
 * @date 2020/3/31
 */
public class LogFile {

    private String fileName;

    private Long firstChange;

    private Long nextChange;

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public Long getFirstChange() {
        return firstChange;
    }

    public void setFirstChange(Long firstChange) {
        this.firstChange = firstChange;
    }

    public Long getNextChange() {
        return nextChange;
    }

    public void setNextChange(Long nextChange) {
        this.nextChange = nextChange;
    }
}
