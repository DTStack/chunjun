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

package com.dtstack.flinkx.oracle.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.oracle.OracleDatabaseMeta;
import com.dtstack.flinkx.rdb.datareader.JdbcDataReader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Oracle reader plugin
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class OracleReader extends JdbcDataReader {

    public OracleReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        setDatabaseInterface(new OracleDatabaseMeta());
    }

    @Override
    protected List<String> descColumnTypes() {
        List<String> ret = new ArrayList<>();

        try (Connection conn = getConnection()) {
            PreparedStatement stmt = conn.prepareStatement("select DATA_TYPE from DBA_TAB_COLUMNS where TABLE_NAME=?");
            stmt.setObject(1, table.toUpperCase());
            ResultSet rs = stmt.executeQuery();
            while(rs.next()) {
                ret.add(rs.getString(1));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return ret;
    }

}
