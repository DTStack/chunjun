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
import com.dtstack.chunjun.sink.WriteMode;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@EqualsAndHashCode(callSuper = true)
@Data
public class ElasticsearchConfig extends CommonConfig implements Serializable {

    private static final long serialVersionUID = -3282191233629067381L;

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
    private SslConfig sslConfig;

    /** Filter condition expression */
    protected Map query;

    /** write mode * */
    private String writeMode = WriteMode.APPEND.name();
}
