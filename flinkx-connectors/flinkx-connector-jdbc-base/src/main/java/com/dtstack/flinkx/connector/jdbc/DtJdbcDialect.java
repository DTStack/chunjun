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
package com.dtstack.flinkx.connector.jdbc;

import org.apache.flink.connector.jdbc.dialect.JdbcDialect;

/**
 * Date: 2021/04/12
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public interface DtJdbcDialect extends JdbcDialect {
    /**
     * Get select fields statement by condition fields. Default use SELECT.
     * @param schemaName
     * @param tableName
     * @param customSql
     * @param selectFields
     * @param where
     * @return
     */
    String getSelectFromStatement(String schemaName, String tableName, String customSql, String[] selectFields, String where);
    /**
     * 获取fetchSize，用以指定一次读取数据条数
     *
     * @return fetchSize
     */
    int getFetchSize();

    /**
     * 获取查询超时时间
     *
     * @return 超时时间
     */
    int getQueryTimeout();
}
