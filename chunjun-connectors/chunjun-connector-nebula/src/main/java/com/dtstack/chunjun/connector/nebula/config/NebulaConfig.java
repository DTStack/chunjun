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

package com.dtstack.chunjun.connector.nebula.config;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.nebula.utils.NebulaSchemaFamily;
import com.dtstack.chunjun.sink.WriteMode;

import com.google.common.collect.Lists;
import com.vesoft.nebula.client.graph.data.HostAddress;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.DSTID;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.RANK;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.SRCID;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.VID;

@EqualsAndHashCode(callSuper = true)
@Data
public class NebulaConfig extends CommonConfig implements Serializable {
    private static final long serialVersionUID = 8347818139367657891L;

    /** nebula password */
    private String password;
    /** nebula storage services addr */
    private List<HostAddress> storageAddresses;
    /** nebula graphd services addr */
    private List<HostAddress> graphdAddresses;
    /** nebula connector timeout */
    private Integer timeout;
    /** nebula user name */
    private String username;
    /** should nebula reconnect */
    private Boolean reconn;
    /** nebula graph tag or relation name */
    private String entityName;
    /** nebula graph primery key */
    private String pk;

    /** the number rows each fatch */
    private Integer fetchSize;
    /** the number rows cache each insert */
    private Integer bulkSize;

    /** nebula graph space */
    private String space;

    /** nebula graph schema component: VERTEX TAG EDGE EDGE_TYPE */
    private NebulaSchemaFamily schemaType;

    /** enable nebula SSL connect */
    private Boolean enableSSL;

    /** SSL type: CASignedSSLParam or SelfSignedSSLParam */
    private NebulaSSLParam sslParamType;

    private String caCrtFilePath;
    private String crtFilePath;
    private String keyFilePath;
    private String sslPassword;

    /** retry connect times */
    private Integer connectionRetry;
    /** retry execute times */
    private Integer executionRetry;

    /** the parallelism of read tasks */
    private Integer readTasks;

    /** the parallelism of write tasks */
    private Integer writeTasks;
    /**
     * Pull data within a given time, and set the fetch-interval to achieve the effect of breakpoint
     * resuming, which is equivalent to dividing the time into multiple pull-up data according to
     * the fetch-interval
     */
    private Long interval;
    /** scan the data after the start-time insert */
    private Long start;
    /** scan the data before the end-time insert */
    private Long end;

    private List<String> columnNames;

    private List<FieldConfig> fields;

    /** if allow part success */
    private Boolean defaultAllowPartSuccess;
    /** if allow read from follower */
    private Boolean defaultAllowReadFollower;

    // The min connections in pool for all addresses
    private Integer minConnsSize;

    // The max connections in pool for all addresses
    private Integer maxConnsSize;

    // The idleTime of the connection, unit: millisecond
    // The connection's idle time more than idleTime, it will be delete
    // 0 means never delete
    private Integer idleTime;

    // the interval time to check idle connection, unit ms, -1 means no check
    private Integer intervalIdle;

    // the wait time to get idle connection, unit ms
    private Integer waitTime;

    private WriteMode mode = WriteMode.INSERT;

    private Integer stringLength;

    private String vidType;

    public List<FieldConfig> getFields() {
        List<String> var = Lists.newArrayList(VID, SRCID, DSTID, RANK);
        if (fields == null) {
            return null;
        }
        return fields.stream()
                .filter(fieldConfig -> !var.contains(fieldConfig.getName()))
                .collect(Collectors.toList());
    }
}
