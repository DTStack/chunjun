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
package com.dtstack.flinkx.connector.jdbc.conf;

import java.util.List;

/**
 * Date: 2021/04/12
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class SourceConnectionConf extends ConnectionConf{

    private String query;
    private String partitionColumnName;
    private Long partitionLowerBound;
    private Long partitionUpperBound;
    private Integer numPartitions;
    private Integer parallelism;

    private int fetchSize;
    private boolean autoCommit;

    protected List<String> jdbcUrl;

    public Integer getParallelism() {
        return parallelism;
    }

    public void setParallelism(Integer parallelism) {
        this.parallelism = parallelism;
    }

    @Override
    public String obtainJdbcUrl() {
        return jdbcUrl.get(0);
    }

    @Override
    public void putJdbcUrl(String jdbcUrl) {
        this.jdbcUrl.set(0, jdbcUrl);
    }

    public List<String> getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(List<String> jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getPartitionColumnName() {
        return partitionColumnName;
    }

    public void setPartitionColumnName(String partitionColumnName) {
        this.partitionColumnName = partitionColumnName;
    }

    public Long getPartitionLowerBound() {
        return partitionLowerBound;
    }

    public void setPartitionLowerBound(Long partitionLowerBound) {
        this.partitionLowerBound = partitionLowerBound;
    }

    public Long getPartitionUpperBound() {
        return partitionUpperBound;
    }

    public void setPartitionUpperBound(Long partitionUpperBound) {
        this.partitionUpperBound = partitionUpperBound;
    }

    public Integer getNumPartitions() {
        return numPartitions;
    }

    public void setNumPartitions(Integer numPartitions) {
        this.numPartitions = numPartitions;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    @Override
    public String toString() {
        return "SourceConnectionConf{" +
                "table=" + table +
                ", schema='" + schema + '\'' +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", query='" + query + '\'' +
                ", partitionColumnName='" + partitionColumnName + '\'' +
                ", partitionLowerBound=" + partitionLowerBound +
                ", partitionUpperBound=" + partitionUpperBound +
                ", numPartitions=" + numPartitions +
                ", parallelism=" + parallelism +
                ", fetchSize=" + fetchSize +
                ", autoCommit=" + autoCommit +
                ", jdbcUrl=" + jdbcUrl +
                '}';
    }
}
