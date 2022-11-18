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

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Provides some methods that can be used to convert data in state, locationValue and statementValue
 *
 * <p>state: formatState
 *
 * <p>locatinValue: startLocation/endLocation
 *
 * <p>statementValue: Representation of data in SQL statements,example str -> 'str'
 */
public abstract class KeyUtil<T, F> implements Serializable {

    private static final long serialVersionUID = -4190152796246863926L;

    public abstract T getSqlValueFromRs(ResultSet rs, int index) throws SQLException;

    public abstract T getSqlValueFromRs(ResultSet rs, String column) throws SQLException;

    public F getLocationValueFromRs(ResultSet rs, int index) throws SQLException {
        Object object = rs.getObject(index);
        if (object == null) {
            return getNullDefaultLocationValue();
        }
        return transToLocationValue(getSqlValueFromRs(rs, index));
    }

    public F getLocationValueFromRs(ResultSet rs, String columnName) throws SQLException {
        Object object = rs.getObject(columnName);
        if (object == null) {
            return getNullDefaultLocationValue();
        }
        return transToLocationValue(getSqlValueFromRs(rs, columnName));
    };

    public abstract void setPsWithSqlValue(PreparedStatement ps, int index, T value)
            throws SQLException;

    public abstract void setPsWithLocationStr(PreparedStatement ps, int index, String value)
            throws SQLException;

    public F transToLocationValue(Object value) {
        if (value == null) {
            return null;
        }
        return transToLocationValueInternal(value);
    }

    public abstract F transToLocationValueInternal(Object value);

    public abstract String transLocationStrToStatementValue(String locationStr);

    public String transToStatementValue(Object sqlValue) {
        if (sqlValue == null) {
            return null;
        }
        return transToStatementValueInternal(sqlValue);
    }

    public abstract String transToStatementValueInternal(Object sqlValue);

    public abstract T transLocationStrToSqlValue(String locationStr);

    public abstract F getNullDefaultLocationValue();

    public abstract String checkAndFormatLocationStr(String originLocationStr);
}
