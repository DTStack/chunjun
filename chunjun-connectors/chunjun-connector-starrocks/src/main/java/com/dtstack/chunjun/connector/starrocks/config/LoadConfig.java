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

package com.dtstack.chunjun.connector.starrocks.config;

import com.dtstack.chunjun.connector.starrocks.options.ConstantValue;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class LoadConfig implements Serializable {

    private static final Long serialVersionUID = 1L;

    /** Timeout duration of the HTTP connection when checking the connectivity with StarRocks */
    private Integer httpCheckTimeoutMs = ConstantValue.HTTP_CHECK_TIMEOUT_DEFAULT;

    private Integer queueOfferTimeoutMs = ConstantValue.QUEUE_OFFER_TIMEOUT_DEFAULT;

    private Integer queuePollTimeoutMs = ConstantValue.QUEUE_POLL_TIMEOUT_DEFAULT;

    private Long batchMaxSize = ConstantValue.SINK_BATCH_MAX_BYTES_DEFAULT;

    private Long batchMaxRows = ConstantValue.SINK_BATCH_MAX_ROWS_DEFAULT;

    private Map<String, String> headProperties = new HashMap<>();

    public Integer getHttpCheckTimeoutMs() {
        return httpCheckTimeoutMs;
    }

    public void setHttpCheckTimeoutMs(Integer httpCheckTimeoutMs) {
        this.httpCheckTimeoutMs = httpCheckTimeoutMs;
    }

    public Integer getQueueOfferTimeoutMs() {
        return queueOfferTimeoutMs;
    }

    public void setQueueOfferTimeoutMs(Integer queueOfferTimeoutMs) {
        this.queueOfferTimeoutMs = queueOfferTimeoutMs;
    }

    public Integer getQueuePollTimeoutMs() {
        return queuePollTimeoutMs;
    }

    public void setQueuePollTimeoutMs(Integer queuePollTimeoutMs) {
        this.queuePollTimeoutMs = queuePollTimeoutMs;
    }

    public Long getBatchMaxSize() {
        return batchMaxSize;
    }

    public void setBatchMaxSize(Long batchMaxSize) {
        this.batchMaxSize = batchMaxSize;
    }

    public Long getBatchMaxRows() {
        return batchMaxRows;
    }

    public void setBatchMaxRows(Long batchMaxRows) {
        this.batchMaxRows = batchMaxRows;
    }

    public Map<String, String> getHeadProperties() {
        return headProperties;
    }

    public void setHeadProperties(Map<String, String> headProperties) {
        this.headProperties = headProperties;
    }
}
