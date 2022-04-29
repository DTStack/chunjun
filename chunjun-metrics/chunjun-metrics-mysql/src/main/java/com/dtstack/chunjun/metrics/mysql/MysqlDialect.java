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

package com.dtstack.chunjun.metrics.mysql;

import com.dtstack.chunjun.metrics.rdb.JdbcDialect;

import java.util.Optional;

/**
 * @program: flinkx
 * @author: shifang
 * @create: 2021/03/17
 */
public class MysqlDialect implements JdbcDialect {

    @Override
    public String dialectName() {
        return "MySQL";
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:mysql:");
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("com.mysql.jdbc.Driver");
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    private String createTableSql =
            "CREATE TABLE if not exists %s.%s (\n"
                    + "  `id` int(11) NOT NULL AUTO_INCREMENT,\n"
                    + "  `job_id` varchar(100) DEFAULT NULL,\n"
                    + "  `job_name` varchar(100) DEFAULT NULL,\n"
                    + "  `task_id` varchar(100) DEFAULT NULL,\n"
                    + "  `task_name` varchar(100) DEFAULT NULL,\n"
                    + "  `subtask_index` varchar(100) DEFAULT NULL,\n"
                    + "  `metric_name` varchar(100) DEFAULT NULL,\n"
                    + "  `metric_value` varchar(100) DEFAULT NULL,\n"
                    + "  `gmt_create` datetime DEFAULT CURRENT_TIMESTAMP,\n"
                    + "  PRIMARY KEY (`id`)\n"
                    + ")";

    @Override
    public String getCreateStatement(String schema, String tableName) {
        return String.format(createTableSql, schema, tableName);
    }
}
