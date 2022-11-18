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

package com.dtstack.chunjun.connector.elasticsearch7;

import com.dtstack.chunjun.config.CommonConfig;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @description:
 * @program: ChunJun
 * @author: lany
 * @create: 2021/06/16 15:36
 */
public class ElasticsearchConf extends CommonConfig implements Serializable {

    private static final long serialVersionUID = 2L;

    /** elasticsearch address -> ip:port localhost:9200 */
    private List<String> hosts;

    /** es index name */
    private String index;

    /** es type name */
    private String type;

    /** es doc id */
    private List<String> ids;

    /** basic auth : username */
    private String username;

    /** basic auth : password */
    private String password;

    private String keyDelimiter = "_";

    /** client socket timeout */
    private int socketTimeout = 1800000;

    /** client keepAlive time */
    private int keepAliveTime = 5000;

    /** client connect timeout */
    private int connectTimeout = 5000;

    /** client request timeout */
    private int requestTimeout = 2000;

    /** Assigns maximum connection per route value. */
    private int maxConnPerRoute = 10;

    /** table field names */
    private String[] fieldNames;

    /** sslConf */
    private SslConf sslConfig;

    /** Filter condition expression */
    protected Map query;

    public String[] getFieldNames() {
        return fieldNames;
    }

    public void setFieldNames(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }

    public int getMaxConnPerRoute() {
        return maxConnPerRoute;
    }

    public void setMaxConnPerRoute(int maxConnPerRoute) {
        this.maxConnPerRoute = maxConnPerRoute;
    }

    public int getRequestTimeout() {
        return requestTimeout;
    }

    public void setRequestTimeout(int requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    public int getKeepAliveTime() {
        return keepAliveTime;
    }

    public void setKeepAliveTime(int keepAliveTime) {
        this.keepAliveTime = keepAliveTime;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public List<String> getHosts() {
        return hosts;
    }

    public void setHosts(List<String> hosts) {
        this.hosts = hosts;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<String> getIds() {
        return ids;
    }

    public void setIds(List<String> ids) {
        this.ids = ids;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getKeyDelimiter() {
        return keyDelimiter;
    }

    public void setKeyDelimiter(String keyDelimiter) {
        this.keyDelimiter = keyDelimiter;
    }

    public SslConf getSslConfig() {
        return sslConfig;
    }

    public void setSslConfig(SslConf sslConfig) {
        this.sslConfig = sslConfig;
    }

    public Map getQuery() {
        return query;
    }

    public void setQuery(Map query) {
        this.query = query;
    }
}
