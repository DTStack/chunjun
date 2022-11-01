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

package com.dtstack.chunjun.connector.doris.options;

import java.io.Serializable;

public class LoadConf implements Serializable {

    private static final Long serialVersionUID = 1L;

    private String readFields;

    private String filterQuery;

    private Integer requestTabletSize;

    private Integer requestConnectTimeoutMs;

    private Integer requestReadTimeoutMs;

    private Integer requestQueryTimeoutS;

    private Integer requestRetries;

    private Integer requestBatchSize;

    private Long execMemLimit;

    private Integer deserializeQueueSize;

    private Boolean deserializeArrowAsync;

    public String getReadFields() {
        return readFields;
    }

    public void setReadFields(String readFields) {
        this.readFields = readFields;
    }

    public String getFilterQuery() {
        return filterQuery;
    }

    public void setFilterQuery(String filterQuery) {
        this.filterQuery = filterQuery;
    }

    public Integer getRequestTabletSize() {
        return requestTabletSize;
    }

    public void setRequestTabletSize(Integer requestTabletSize) {
        this.requestTabletSize = requestTabletSize;
    }

    public Integer getRequestConnectTimeoutMs() {
        return requestConnectTimeoutMs;
    }

    public void setRequestConnectTimeoutMs(Integer requestConnectTimeoutMs) {
        this.requestConnectTimeoutMs = requestConnectTimeoutMs;
    }

    public Integer getRequestReadTimeoutMs() {
        return requestReadTimeoutMs;
    }

    public void setRequestReadTimeoutMs(Integer requestReadTimeoutMs) {
        this.requestReadTimeoutMs = requestReadTimeoutMs;
    }

    public Integer getRequestQueryTimeoutS() {
        return requestQueryTimeoutS;
    }

    public void setRequestQueryTimeoutS(Integer requestQueryTimeoutS) {
        this.requestQueryTimeoutS = requestQueryTimeoutS;
    }

    public Integer getRequestRetries() {
        return requestRetries;
    }

    public void setRequestRetries(Integer requestRetries) {
        this.requestRetries = requestRetries;
    }

    public Integer getRequestBatchSize() {
        return requestBatchSize;
    }

    public void setRequestBatchSize(Integer requestBatchSize) {
        this.requestBatchSize = requestBatchSize;
    }

    public Long getExecMemLimit() {
        return execMemLimit;
    }

    public void setExecMemLimit(Long execMemLimit) {
        this.execMemLimit = execMemLimit;
    }

    public Integer getDeserializeQueueSize() {
        return deserializeQueueSize;
    }

    public void setDeserializeQueueSize(Integer deserializeQueueSize) {
        this.deserializeQueueSize = deserializeQueueSize;
    }

    public Boolean getDeserializeArrowAsync() {
        return deserializeArrowAsync;
    }

    public void setDeserializeArrowAsync(Boolean deserializeArrowAsync) {
        this.deserializeArrowAsync = deserializeArrowAsync;
    }
}
