/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.metadataes6.constants;

/**
 * @author : baiyu
 * @date : 2020/12/3
 */
public class MetaDataEs6Cons {

    public static final String KEY_INDICES = "indices";

    public static final String KEY_URL = "url";

    public static final String KEY_USERNAME = "username";

    public static final String KEY_PASSWORD = "password";

    public static final String KEY_INDEX = "index";

    /**
     *  字段映射
     */
    public static final String KEY_FIELDS = "fields";

    /**
     *  API_*表示restAPI请求前缀
     */
    public static final String API_METHOD_GET = "GET";

    public static final String API_ENDPOINT_CAT_INDEX = "/_cat/indices";

    /**
     * Map_*表示es查询得到的map的关键字key
     */
    public static final String MAP_SETTINGS = "settings";

    public static final String MAP_CREATION_DATE = "creation_date";

    public static final String MAP_NUMBER_OF_SHARDS = "number_of_shards";

    public static final String MAP_NUMBER_OF_REPLICAS = "number_of_replicas";

    public static final String MAP_MAPPINGS = "mappings";

    public static final String MAP_PROPERTIES = "properties";

    public static final String MAP_TYPE = "type";

    public static final String MAP_ALIASES = "aliases";

}
