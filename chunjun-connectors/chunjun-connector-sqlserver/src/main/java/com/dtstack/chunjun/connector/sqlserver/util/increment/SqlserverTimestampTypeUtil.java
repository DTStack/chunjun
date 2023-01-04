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

package com.dtstack.chunjun.connector.sqlserver.util.increment;

import com.dtstack.chunjun.connector.jdbc.util.key.KeyUtil;
import com.dtstack.chunjun.connector.sqlserver.util.SqlUtil;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.commons.lang3.StringUtils;

import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SqlserverTimestampTypeUtil extends KeyUtil<byte[], BigInteger> {

    private static final long serialVersionUID = 3245211271408129356L;

    @Override
    public byte[] getSqlValueFromRs(ResultSet rs, int index) throws SQLException {
        return rs.getBytes(index);
    }

    @Override
    public byte[] getSqlValueFromRs(ResultSet rs, String column) throws SQLException {
        return rs.getBytes(column);
    }

    @Override
    public void setPsWithSqlValue(PreparedStatement ps, int index, byte[] value)
            throws SQLException {
        ps.setBytes(index, value);
    }

    @Override
    public void setPsWithLocationStr(PreparedStatement ps, int index, String locationStr)
            throws SQLException {
        ps.setBytes(index, SqlUtil.longToTimestampBytes(Long.parseLong(locationStr)));
    }

    @Override
    public BigInteger transToLocationValueInternal(Object value) {
        return BigInteger.valueOf(SqlUtil.timestampBytesToLong((byte[]) value));
    }

    @Override
    public String transLocationStrToStatementValue(String locationStr) {
        return locationStr;
    }

    @Override
    public String transToStatementValueInternal(Object sqlValue) {
        return String.valueOf(SqlUtil.timestampBytesToLong((byte[]) sqlValue));
    }

    @Override
    public byte[] transLocationStrToSqlValue(String locationStr) {
        return SqlUtil.longToTimestampBytes(Long.parseLong(locationStr));
    }

    @Override
    public BigInteger getNullDefaultLocationValue() {
        return BigInteger.valueOf(Long.MIN_VALUE);
    }

    @Override
    public String checkAndFormatLocationStr(String originLocationStr) {
        if (!StringUtils.isNumeric(originLocationStr)) {
            throw new ChunJunRuntimeException(
                    String.format(
                            "The Sqlserver timestamp type locationValue is expected to be of type long but it is %s",
                            originLocationStr));
        }
        return originLocationStr;
    }
}
