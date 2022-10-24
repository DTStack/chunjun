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

package com.dtstack.chunjun.restore.mysql.constant;

public class SqlConstants {

    // 外部数据源字段名
    public static final String DATABASE_NAME_KEY = "database_name";

    public static final String SCHEMA_NAME_KEY = "schema_name";

    public static final String TABLE_NAME_KEY = "table_name";

    public static final String DATABASE_KEY = "database";

    public static final String TABLE_KEY = "table";

    public static final String FIND_DDL_CHANGED =
            "SELECT database_name, schema_name, table_name FROM `$database`.`$table`"
                    + " WHERE status = 2 AND is_delete = 0";

    public static final String FIND_DDL_UNCHANGED =
            "SELECT database_name, schema_name, table_name FROM `$database`.`$table`"
                    + " WHERE status != 2 and is_delete = 0";

    public static final String INSERT_DDL_CHANGE =
            "INSERT INTO `$database`.`$table` "
                    + "(database_name, schema_name, table_name, operation, lsn, lsn_sequence, content, convented_content, update_time, status, error_info)"
                    + " VALUE (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?)";

    public static final String UPDATE_DDL_CHANGE =
            "UPDATE `$database`.`$table` SET status = ?, error_info = ?"
                    + " WHERE database_name = ? AND schema_name = ? AND table_name = ? AND lsn = ? AND lsn_sequence = ?  AND is_delete = 0";

    public static final String UPDATE_DDL_CHANGE_DATABASE_NULLABLE =
            "UPDATE `$database`.`$table` SET status = ? , error_info = ?"
                    + " WHERE  database_name IS NULL AND schema_name = ? AND table_name = ? AND lsn = ? AND lsn_sequence = ?  AND is_delete = 0";

    public static final String SEND_CACHE =
            "INSERT INTO `$database`.`$table` (database_name, schema_name, table_name, operation, lsn, lsn_sequence, content) values (?, ?, ?, ?, ?, ?, ?)";

    public static final String SELECT_CACHE =
            "SELECT * FROM `$database`.`$table` WHERE database_name = ? AND schema_name = ? AND table_name = ? AND is_delete = 0 ORDER BY lsn, lsn_sequence  LIMIT ?;";

    public static final String SELECT_CACHE_DATABASE_NULLABLE =
            "SELECT * FROM `$database`.`$table` WHERE database_name IS NULL AND schema_name = ? AND table_name = ? AND is_delete = 0 ORDER BY lsn, lsn_sequence  LIMIT ?;";

    public static final String DELETE_CHANGED_DDL =
            "UPDATE `$database`.`$table` SET is_delete = 1"
                    + " WHERE database_name = ? AND schema_name = ? AND table_name = ? AND status = 2 AND is_delete = 0";

    public static final String DELETE_CHANGED_DDL_DATABASE_NULLABLE =
            "UPDATE `$database`.`$table` SET is_delete = 1"
                    + " WHERE database_name IS NULL AND schema_name = ? AND table_name = ? AND status = 2 AND is_delete = 0";

    public static final String DELETE_PROCESSED_CACHE =
            "UPDATE `$database`.`$table` SET is_delete = 1"
                    + " WHERE database_name = ? AND schema_name = ? AND table_name = ?  AND ( lsn < ? OR (lsn = ? AND lsn_sequence <= ?) )  AND is_delete = 0";

    public static final String DELETE_PROCESSED_CACHE_DATABASE_NULLABLE =
            "UPDATE `$database`.`$table` SET is_delete = 1"
                    + " WHERE database_name IS NULL AND schema_name = ? AND table_name = ?   AND ( lsn < ? OR (lsn = ? AND lsn_sequence <= ?) )  AND is_delete = 0";
}
