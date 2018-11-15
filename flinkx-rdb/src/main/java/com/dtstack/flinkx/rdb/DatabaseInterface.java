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

package com.dtstack.flinkx.rdb;

import com.dtstack.flinkx.enums.EDatabaseType;

import java.util.List;
import java.util.Map;

/**
 * Database prototype specification
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public interface DatabaseInterface {

    EDatabaseType getDatabaseType();

    String getDriverClass();

    String getSQLQueryFields(String tableName);

    String getSQLQueryColumnFields(List<String> column, String table);

    String getStartQuote();

    String getEndQuote();

    String quoteColumn(String column);

    String quoteColumns(List<String> column, String table);

    String quoteColumns(List<String> column);

    String quoteTable(String table);

    String getInsertStatement(List<String> column, String table);

    String getReplaceStatement(List<String> column, List<String> fullColumn, String table, Map<String,List<String>> updateKey);

    String getUpsertStatement(List<String> column, String table, Map<String,List<String>> updateKey);

    String getMultiInsertStatement(List<String> column, String table, int batchSize);

    String getMultiReplaceStatement(List<String> column, List<String> fullColumn, String table, int batchSize, Map<String,List<String>> updateKey);

    String getMultiUpsertStatement(List<String> column, String table, int batchSize, Map<String,List<String>> updateKey);

    String getSplitFilter(String columnName);

    int getFetchSize();

    int getQueryTimeout();
}
