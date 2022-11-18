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

package com.dtstack.chunjun.connector.nebula.utils;

public class NebulaConstant {

    // template for insert statement
    public static String BATCH_INSERT_TEMPLATE = "INSERT %s `%s`(%s) VALUES %s";
    public static String VERTEX_VALUE_TEMPLATE = "%s: (%s)";
    public static String EDGE_VALUE_WITHOUT_RANKING_TEMPLATE = "%s->%s: (%s)";
    public static String EDGE_VALUE_TEMPLATE = "%s->%s@%d: (%s)";

    // template for update statement
    public static String UPDATE_VERTEX_TEMPLATE = "UPDATE %s ON `%s` %s SET %s";
    public static String UPDATE_EDGE_TEMPLATE = "UPDATE %s ON `%s` %s->%s@%d SET %s";
    public static String UPDATE_EDGE_WITHOUT_RANK_TEMPLATE = "UPDATE %s ON `%s` %s->%s SET %s";
    public static String UPDATE_VALUE_TEMPLATE = "`%s`=%s";

    // template for upsert statement
    public static String UPSERT_VERTEX_TEMPLATE = "UPSERT %s ON `%s` %s SET %s";
    public static String UPSERT_EDGE_TEMPLATE = "UPSERT %s ON `%s` %s->%s@%d SET %s";
    public static String UPSERT_EDGE_WITHOUT_RANK_TEMPLATE = "UPSERT %s ON `%s` %s->%s SET %s";
    public static String UPSERT_VALUE_TEMPLATE = "`%s`=%s";

    // template for delete statement
    public static String DELETE_VERTEX_TEMPLATE = "DELETE VERTEX %s";
    public static String DELETE_EDGE_TEMPLATE = "DELETE EDGE `%s` %s";
    public static String EDGE_ENDPOINT_TEMPLATE = "%s->%s@%d";
    public static String EDGE_ENDPOINT_WITHOUT_RANK_TEMPLATE = "%s->%s";

    // template for create space statement
    public static String NEBULA_PROP = "`%s` %s";
    public static String CREATE_VERTEX = "CREATE TAG IF NOT EXISTS `%s` (%s)";
    public static String CREATE_EDGE = "CREATE EDGE IF NOT EXISTS `%s` (%s)";

    public static final String VID = "vid";
    public static final String SRCID = "srcId";
    public static final String DSTID = "dstId";
    public static final String RANK = "rank";

    public static final String TAG = "tag";
    public static final String VERTEX = "vertex";
    public static final String EDGE = "edge";
    public static final String EDGE_TYPE = "edge_type";

    public static final String SELF = "self";
    public static final String CAS = "cas";

    public static final String DELIMITERS = ",";
    public static final String SUB_DELIMITERS = ":";

    public static final String INSERT = "insert";
    public static final String UPSERT = "upsert";
}
