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

package com.dtstack.flinkx.restore.mysql;

/**
 * @author tiezhu@dtstack.com
 * @since 2021/12/21 星期二
 */
public class MysqlFetcherConstant {

    private MysqlFetcherConstant() {}

    public static final String SELECT =
            "select database_name, table_name from `$database`.`$table`"
                    + " where status = 2 and database_name = ? and table_name = ? and lsn <= ?";

    public static final String DELETE =
            "delete from `$database`.`$table`"
                    + " where status = 2 and database_name = ? and table_name = ? and lsn <= ?";

    public static final String QUERY =
            "select database_name, table_name, operation_type, lsn, content, update_time from `$database`.`$table`"
                    + " where status != 2";

    public static final String INSERT =
            "INSERT INTO `$database`.`$table` "
                    + "(database_name, table_name, operation_type, lsn, content, update_time, status)"
                    + " VALUE ($database_name, $table_name, '$operation_type', '$lsn', '$content', $update_time, 0)";

    public static final String SELECT_CHECK = "SELECT * FROM `$database`.`$table` WHERE 1 = 2";

    public static final String DELETE_CHECK = "DELETE FROM `$database`.`$table` WHERE 1 = 2";

    public static final String INSERT_CHECK =
            "INSERT INTO `$database`.`$table` (SELECT * FROM `$database`.`$table` WHERE 1 = 2)";

    public static final String DATABASE_KEY = "database";

    public static final String TABLE_KEY = "table";

    public static final String DRIVER = "com.mysql.jdbc.Driver";
}
