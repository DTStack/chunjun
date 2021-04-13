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

package com.dtstack.flinkx.connector.mysql;

import com.dtstack.flinkx.connector.jdbc.converter.AbstractJdbcRowConverter;

import com.dtstack.flinkx.util.DateUtil;
import org.apache.commons.lang3.StringUtils;

import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.util.Locale;

/**
 * @program: luna-flink
 * @author: wuren
 * @create: 2021/03/29
 **/
public class MySQLRowConverter extends AbstractJdbcRowConverter {

    private static final long serialVersionUID = 1L;

    // TODO 是否需要删除
    public String converterName() {
        return "MySQL";
    }

    public MySQLRowConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    public JdbcDeserializationConverter createJdbcInternalConverter(String type) throws SQLException {
        if(StringUtils.isBlank(type)){
            return (resultSet, pos) -> resultSet.getObject(pos);
        }
        return null;
        //MySQL支持的数据类型: com.mysql.jdbc.MysqlDefs
        //com.mysql.jdbc.ResultSetImpl.getObject(int)
        //TODO 仔细梳理每个数据库支持的数据类型
//        switch (type.toUpperCase(Locale.ENGLISH)){
//            case "BIT":
//                return (resultSet, pos) -> resultSet.getBoolean(pos) ? 1 : 0;
//            case "TINYINT":
//                return (int)resultSet.getByte(index);
//            case "SMALLINT":
//                return resultSet.getInt(index);
//            case "MEDIUMINT":
//                return resultSet.getInt(index);
//            case "INT":
//                return resultSet.getObject(index);
//            case "INTEGER":
//                return resultSet.getObject(index);
//            case "BIGINT":
//                return resultSet.getObject(index);
//            case "INT24":
//                return resultSet.getInt(index);
//            case "REAL":
//                return resultSet.getFloat(index);
//            case "FLOAT":
//                return resultSet.getDouble(index);
//            case "DECIMAL":
//            case "NUMERIC":
//                return resultSet.getObject(index);
//            case "DOUBLE":
//                return resultSet.getDouble(index);
//            case "CHAR":
//            case "VARCHAR":
//                return resultSet.getObject(index);
//            case "DATE":
//                return resultSet.getObject(index);
//            case "TIME":
//                return resultSet.getTime(index);
//            case "YEAR":
//                Time time = resultSet.getTime(index);
//                return StringData.fromString(DateUtil.dateToYearString(time));
//            case "TIMESTAMP":
//                return resultSet.getTimestamp(index);
//            case "DATETIME":
//                return resultSet.getString(index);
//            case "TINYBLOB":
//                return resultSet.getObject(index);
//            case "BLOB":
//                return resultSet.getObject(index);
//            case "MEDIUMBLOB":
//                return resultSet.getObject(index);
//            case "LONGBLOB":
//                return resultSet.getObject(index);
//            case "TINYTEXT":
//                return resultSet.getObject(index);
//            case "TEXT":
//                return resultSet.getObject(index);
//            case "MEDIUMTEXT":
//                return resultSet.getObject(index);
//            case "LONGTEXT":
//                return resultSet.getObject(index);
//            case "ENUM":
//                return resultSet.getObject(index);
//            case "SET":
//                return resultSet.getObject(index);
//            case "GEOMETRY":
//                return resultSet.getObject(index);
//            case "BINARY":
//                return resultSet.getObject(index);
//            case "VARBINARY":
//                return resultSet.getObject(index);
//            case "JSON":
//                return resultSet.getObject(index);
//
//            default:
//                return resultSet.getString(index);
//        }
    }
}
