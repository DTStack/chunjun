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

package com.dtstack.flinkx.sqlserver.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.rdb.datareader.JdbcDataReader;
import com.dtstack.flinkx.sqlserver.SqlServerDatabaseMeta;
import com.dtstack.flinkx.util.ClassUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * SQLServer reader plugin
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class SqlserverReader extends JdbcDataReader {

    public SqlserverReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        setDatabaseInterface(new SqlServerDatabaseMeta());
    }

    @Override
    protected List<String> descColumnTypes() {
        List<String> ret = new ArrayList<>();
        ClassUtil.forName(databaseInterface.getDriverClass(), this.getClass().getClassLoader());
        DriverManager.setLoginTimeout(10);
        Connection conn = null;

        try {
            conn = DriverManager.getConnection(dbUrl, username, password);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(databaseInterface.getSQLQueryFields(table));
            ResultSetMetaData rd = rs.getMetaData();
            for(int i = 0; i < rd.getColumnCount(); ++i) {
                ret.add(rd.getColumnTypeName(i+1));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if(conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        return ret;
    }

}
