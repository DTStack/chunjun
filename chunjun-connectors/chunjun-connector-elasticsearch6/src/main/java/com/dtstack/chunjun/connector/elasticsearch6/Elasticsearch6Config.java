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

package com.dtstack.chunjun.connector.elasticsearch6;

import com.dtstack.chunjun.config.CommonConfig;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
public class Elasticsearch6Config extends CommonConfig implements Serializable {

    private static final long serialVersionUID = 5325755340503264018L;

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
}
