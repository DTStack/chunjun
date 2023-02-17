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

import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class NumericTypeUtil extends KeyUtil<Long, BigInteger> {

    private static final long serialVersionUID = -8216714135018785606L;

    @Override
    public Long getSqlValueFromRs(ResultSet rs, int index) throws SQLException {
        return new BigDecimal(rs.getString(index)).toBigInteger().longValue();
    }

    @Override
    public Long getSqlValueFromRs(ResultSet rs, String columnName) throws SQLException {
        return new BigDecimal(rs.getString(columnName)).toBigInteger().longValue();
    }

    @Override
    public void setPsWithSqlValue(PreparedStatement ps, int index, Long value) throws SQLException {
        ps.setLong(index, value);
    }

    @Override
    public void setPsWithLocationStr(PreparedStatement ps, int index, String value)
            throws SQLException {
        ps.setLong(index, Long.parseLong(value));
    }

    @Override
    public BigInteger transToLocationValueInternal(Object value) {
        return new BigDecimal(String.valueOf(value)).toBigInteger();
    }

    @Override
    public String transLocationStrToStatementValue(String locationStr) {
        return locationStr;
    }

    @Override
    public String transToStatementValueInternal(Object sqlValue) {
        return sqlValue.toString();
    }

    @Override
    public Long transLocationStrToSqlValue(String locationStr) {
        return Long.parseLong(locationStr);
    }

    @Override
    public BigInteger getNullDefaultLocationValue() {
        return BigInteger.valueOf(Long.MIN_VALUE);
    }

    @Override
    public String checkAndFormatLocationStr(String originLocationStr) {
        if (!StringUtils.isNumeric(originLocationStr)) {
            throw new ChunJunRuntimeException(
                    "Non-numeric location values are not supported when incrementColumnType is numeric");
        }
        return originLocationStr;
    }
}
