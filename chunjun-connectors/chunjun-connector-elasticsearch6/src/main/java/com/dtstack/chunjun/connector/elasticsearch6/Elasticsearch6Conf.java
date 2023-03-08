package com.dtstack.chunjun.connector.elasticsearch6; /*
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

import com.dtstack.chunjun.conf.ChunJunCommonConf;

import java.io.Serializable;
import java.util.List;

/**
 * @description:
 * @program chunjun
 * @author: lany
 * @create: 2021/06/16 15:36
 */
public class Elasticsearch6Conf extends ChunJunCommonConf implements Serializable {

    private static final long serialVersionUID = 2L;

    /** elasticsearch address -> ip:port localhost:9200 */
    private List<String> hosts;

    /** es index name */
    private String index;

    /** es type name */
    private String type;

    /** es doc id */
    private List<String> ids;

    /** is open basic auth. */
    private boolean authMesh = false;

    /** basic auth : username */
    private String username;

    /** basic auth : password */
    private String password;

    private String keyDelimiter = "_";

    /** table field names */
    private String[] fieldNames;

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

    public String[] getFieldNames() {
        return fieldNames;
    }

    public void setFieldNames(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }
}
