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

package com.dtstack.chunjun.connector.kudu.conf;

import com.dtstack.chunjun.lookup.conf.LookupConf;

import org.apache.flink.configuration.ReadableConfig;

import static com.dtstack.chunjun.connector.kudu.table.KuduOptions.FAULT_TOLERANT;
import static com.dtstack.chunjun.connector.kudu.table.KuduOptions.LIMIT_NUM;
import static com.dtstack.chunjun.connector.kudu.table.KuduOptions.SCANNER_BATCH_SIZE_BYTES;
import static com.dtstack.chunjun.connector.kudu.table.KuduOptions.TABLE_NAME;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_ASYNC_TIMEOUT;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_MAX_ROWS;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_PERIOD;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_TTL;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_TYPE;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_ERROR_LIMIT;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_FETCH_SIZE;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_MAX_RETRIES;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_PARALLELISM;

/**
 * @author tiezhu
 * @since 2021/6/17 星期四
 */
public class KuduLookupConf extends LookupConf {

    private KuduCommonConf commonConf;

    private Integer batchSizeBytes;

    private Long limitNum;

    private Boolean isFaultTolerant;

    public KuduCommonConf getCommonConf() {
        return commonConf;
    }

    public void setCommonConf(KuduCommonConf commonConf) {
        this.commonConf = commonConf;
    }

    public Integer getBatchSizeBytes() {
        return batchSizeBytes;
    }

    public void setBatchSizeBytes(Integer batchSizeBytes) {
        this.batchSizeBytes = batchSizeBytes;
    }

    public Long getLimitNum() {
        return limitNum;
    }

    public void setLimitNum(Long limitNum) {
        this.limitNum = limitNum;
    }

    public Boolean getFaultTolerant() {
        return isFaultTolerant;
    }

    public void setFaultTolerant(Boolean faultTolerant) {
        isFaultTolerant = faultTolerant;
    }

    public static KuduLookupConf from(ReadableConfig readableConfig) {
        KuduLookupConf conf = new KuduLookupConf();

        KuduCommonConf kuduCommonConf = KuduCommonConf.from(readableConfig, new KuduCommonConf());
        conf.setCommonConf(kuduCommonConf);

        // common lookup
        conf.setTableName(readableConfig.get(TABLE_NAME));
        conf.setPeriod(readableConfig.get(LOOKUP_CACHE_PERIOD));
        conf.setCacheSize(readableConfig.get(LOOKUP_CACHE_MAX_ROWS));
        conf.setCacheTtl(readableConfig.get(LOOKUP_CACHE_TTL));
        conf.setCache(readableConfig.get(LOOKUP_CACHE_TYPE));
        conf.setMaxRetryTimes(readableConfig.get(LOOKUP_MAX_RETRIES));
        conf.setErrorLimit(readableConfig.get(LOOKUP_ERROR_LIMIT));
        conf.setFetchSize(readableConfig.get(LOOKUP_FETCH_SIZE));
        conf.setAsyncTimeout(readableConfig.get(LOOKUP_ASYNC_TIMEOUT));
        conf.setParallelism(readableConfig.get(LOOKUP_PARALLELISM));

        // kudu lookup
        conf.setBatchSizeBytes(readableConfig.get(SCANNER_BATCH_SIZE_BYTES));
        conf.setLimitNum(readableConfig.get(LIMIT_NUM));
        conf.setFaultTolerant(readableConfig.get(FAULT_TOLERANT));

        return conf;
    }

    @Override
    public String toString() {
        return "KuduLookupConf{"
                + "commonConf="
                + commonConf
                + ", batchSizeBytes="
                + batchSizeBytes
                + ", limitNum="
                + limitNum
                + ", isFaultTolerant="
                + isFaultTolerant
                + ", tableName='"
                + tableName
                + '\''
                + ", period="
                + period
                + ", cacheSize="
                + cacheSize
                + ", cacheTtl="
                + cacheTtl
                + ", cache='"
                + cache
                + '\''
                + ", maxRetryTimes="
                + maxRetryTimes
                + ", errorLimit="
                + errorLimit
                + ", fetchSize="
                + fetchSize
                + ", asyncTimeout="
                + asyncTimeout
                + ", parallelism="
                + parallelism
                + '}';
    }
}
