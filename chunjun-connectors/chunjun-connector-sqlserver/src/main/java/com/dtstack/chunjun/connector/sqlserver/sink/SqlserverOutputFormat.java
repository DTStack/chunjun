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

package com.dtstack.chunjun.connector.sqlserver.sink;

import com.dtstack.chunjun.connector.jdbc.sink.JdbcOutputFormat;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.connector.sqlserver.dialect.SqlserverDialect;
import com.dtstack.chunjun.throwable.FlinkxRuntimeException;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/5/19 20:03
 */
public class SqlserverOutputFormat extends JdbcOutputFormat {

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        super.openInternal(taskNumber, numTasks);

        Statement statement = null;
        String sql =
                ((SqlserverDialect) jdbcDialect)
                        .getIdentityInsertOnSql(jdbcConf.getSchema(), jdbcConf.getTable());
        try {
            statement = dbConn.createStatement();
            statement.execute(sql);
        } catch (SQLException e) {
            throw new FlinkxRuntimeException(e);
        } finally {
            JdbcUtil.closeDbResources(null, statement, null, false);
        }
    }
}
