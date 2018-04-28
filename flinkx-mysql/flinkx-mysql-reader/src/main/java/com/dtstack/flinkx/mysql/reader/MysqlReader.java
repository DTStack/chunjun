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

package com.dtstack.flinkx.mysql.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.mysql.MySqlDatabaseMeta;
import com.dtstack.flinkx.rdb.datareader.JdbcDataReader;
import com.dtstack.flinkx.util.ClassUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * MySQL reader plugin
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class MysqlReader extends JdbcDataReader {

    public MysqlReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        setDatabaseInterface(new MySqlDatabaseMeta());
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
            ResultSet rs = stmt.executeQuery("desc " + databaseInterface.getStartQuote() + table + databaseInterface.getEndQuote());
            while(rs.next()) {
                String typeName = rs.getString(2);
                int index = typeName.indexOf("(");
                if(index != -1) {
                    typeName = typeName.substring(0, index);
                }
                ret.add(typeName);
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
