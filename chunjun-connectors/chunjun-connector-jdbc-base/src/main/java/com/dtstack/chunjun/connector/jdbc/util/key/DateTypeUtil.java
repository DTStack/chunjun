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

import java.math.BigInteger;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DateTypeUtil extends KeyUtil<Date, BigInteger> {

    private static final long serialVersionUID = 3421622481648892359L;

    @Override
    public Date getSqlValueFromRs(ResultSet rs, int index) throws SQLException {
        return rs.getDate(index);
    }

    @Override
    public Date getSqlValueFromRs(ResultSet rs, String columnName) throws SQLException {
        return rs.getDate(columnName);
    }

    @Override
    public void setPsWithSqlValue(PreparedStatement ps, int index, Date value) throws SQLException {
        ps.setDate(index, value);
    }

    @Override
    public void setPsWithLocationStr(PreparedStatement ps, int index, String value)
            throws SQLException {
        ps.setDate(index, new Date(Long.parseLong(value)));
    }

    @Override
    public BigInteger transToLocationValueInternal(Object value) {
        return BigInteger.valueOf(((Date) value).getTime());
    }

    @Override
    public String transLocationStrToStatementValue(String locationStr) {
        return String.format("'%s'", transLocationStrToSqlValue(locationStr));
    }

    @Override
    public String transToStatementValueInternal(Object sqlValue) {
        return String.format("'%s'", sqlValue);
    }

    @Override
    public Date transLocationStrToSqlValue(String locationStr) {
        return new Date(Long.parseLong(locationStr));
    }

    @Override
    public BigInteger getNullDefaultLocationValue() {
        return BigInteger.valueOf(Long.MIN_VALUE);
    }

    @Override
    public String checkAndFormatLocationStr(String originLocationStr) {
        if (!StringUtils.isNumeric(originLocationStr)) {
            try {
                return String.valueOf(Date.valueOf(originLocationStr).getTime());
            } catch (Exception e) {
                throw new ChunJunRuntimeException(
                        String.format(
                                "failed cast locationStr[%s] to Date,"
                                        + "Please check location is a valid date string like yyyy-MM-dd",
                                originLocationStr));
            }
        }
        return originLocationStr;
    }
}
