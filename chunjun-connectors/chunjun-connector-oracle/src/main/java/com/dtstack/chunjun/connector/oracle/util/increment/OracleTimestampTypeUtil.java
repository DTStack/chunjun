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

package com.dtstack.chunjun.connector.oracle.util.increment;

import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.connector.jdbc.util.key.TimestampTypeUtil;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

public class OracleTimestampTypeUtil extends TimestampTypeUtil {

    private static final long serialVersionUID = 8359654027696724132L;

    @Override
    public void setPsWithLocationStr(PreparedStatement ps, int index, String value)
            throws SQLException {
        ps.setString(index, transLocationStrToStatementValue(value));
    }

    @Override
    public void setPsWithSqlValue(PreparedStatement ps, int index, Timestamp value)
            throws SQLException {
        ps.setString(index, transLocationStrToStatementValue(String.valueOf(value.getTime())));
    }

    @Override
    public String transLocationStrToStatementValue(String locationStr) {
        String timeStr;
        Timestamp ts = new Timestamp(JdbcUtil.getMillis(Long.parseLong(locationStr)));
        ts.setNanos(JdbcUtil.getNanos(Long.parseLong(locationStr)));
        timeStr = JdbcUtil.getNanosTimeStr(ts.toString());
        timeStr = timeStr.substring(0, 23);
        return String.format("TO_TIMESTAMP('%s','yyyy-MM-dd HH24:mi:ss.FF6')", timeStr);
    }
}
