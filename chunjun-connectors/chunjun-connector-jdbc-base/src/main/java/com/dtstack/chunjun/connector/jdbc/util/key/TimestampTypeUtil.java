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

package com.dtstack.chunjun.connector.jdbc.util.key;

import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.commons.lang3.StringUtils;

import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class TimestampTypeUtil extends KeyUtil<Timestamp, BigInteger> {

    private static final long serialVersionUID = -3625527870391581143L;

    @Override
    public Timestamp getSqlValueFromRs(ResultSet rs, int index) throws SQLException {
        return rs.getTimestamp(index);
    }

    @Override
    public Timestamp getSqlValueFromRs(ResultSet rs, String columnName) throws SQLException {
        return rs.getTimestamp(columnName);
    }

    @Override
    public void setPsWithSqlValue(PreparedStatement ps, int index, Timestamp value)
            throws SQLException {
        ps.setTimestamp(index, value);
    }

    @Override
    public void setPsWithLocationStr(PreparedStatement ps, int index, String value)
            throws SQLException {
        ps.setString(index, transLocationStrToTimeStr(value));
    }

    @Override
    public BigInteger transToLocationValueInternal(Object value) {
        return BigInteger.valueOf(((Timestamp) value).getTime());
    }

    @Override
    public String transLocationStrToStatementValue(String locationStr) {
        return String.format("'%s'", transLocationStrToTimeStr(locationStr));
    }

    @Override
    public String transToStatementValueInternal(Object sqlValue) {
        return transLocationStrToStatementValue(String.valueOf(((Timestamp) sqlValue).getTime()));
    }

    @Override
    public Timestamp transLocationStrToSqlValue(String locationStr) {
        return new Timestamp(Long.parseLong(locationStr));
    }

    @Override
    public BigInteger getNullDefaultLocationValue() {
        return BigInteger.valueOf(Long.MIN_VALUE);
    }

    @Override
    public String checkAndFormatLocationStr(String originLocationStr) {
        if (!StringUtils.isNumeric(originLocationStr)) {
            try {
                return String.valueOf(Timestamp.valueOf(originLocationStr).getTime());
            } catch (Exception e) {
                throw new ChunJunRuntimeException(
                        String.format(
                                "failed cast locationStr[%s] to Timestamp,"
                                        + "Please check location is a valid timestamp string like yyyy-MM-dd HH:mm:ss",
                                originLocationStr));
            }
        }
        return originLocationStr;
    }

    public String transLocationStrToTimeStr(String timestampLongStr) {
        long timestampLong = Long.parseLong(timestampLongStr);
        String timestampStr;
        Timestamp ts = new Timestamp(JdbcUtil.getMillis(timestampLong));
        ts.setNanos(JdbcUtil.getNanos(timestampLong));
        timestampStr = JdbcUtil.getNanosTimeStr(ts.toString());
        return timestampStr.substring(0, 23);
    }
}
