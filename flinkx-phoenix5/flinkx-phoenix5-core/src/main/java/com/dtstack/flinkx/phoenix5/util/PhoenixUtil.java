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
package com.dtstack.flinkx.phoenix5.util;

import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.TelnetUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PFloat;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PSmallint;
import org.apache.phoenix.schema.types.PTime;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PTinyint;
import org.apache.phoenix.schema.types.PUnsignedDate;
import org.apache.phoenix.schema.types.PUnsignedDouble;
import org.apache.phoenix.schema.types.PUnsignedFloat;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PUnsignedLong;
import org.apache.phoenix.schema.types.PUnsignedSmallint;
import org.apache.phoenix.schema.types.PUnsignedTime;
import org.apache.phoenix.schema.types.PUnsignedTimestamp;
import org.apache.phoenix.schema.types.PUnsignedTinyint;
import org.apache.phoenix.schema.types.PVarchar;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ClassBodyEvaluator;

import javax.ws.rs.NotSupportedException;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Date: 2020/02/28
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class PhoenixUtil {

    public static Connection getConnectionInternal(String url, Properties properties, ClassLoader parentClassLoader) throws SQLException, IOException, CompileException {
        Connection dbConn;
        synchronized (ClassUtil.LOCK_STR) {
            DriverManager.setLoginTimeout(10);

            // telnet
            TelnetUtil.telnet(url);
            ClassBodyEvaluator cbe = new ClassBodyEvaluator();
            cbe.setParentClassLoader(parentClassLoader);
            cbe.setDefaultImports("java.sql.Connection", "java.sql.DriverManager", "java.sql.SQLException", "java.util.Properties");
            cbe.setImplementedInterfaces(new Class[]{IPhoenixConn.class});
            StringReader sr = new StringReader("public Connection getConn(String url, Properties properties) throws SQLException { return DriverManager.getConnection(url, properties); }");
            IPhoenixConn iPhoenixConn = (IPhoenixConn) cbe.createInstance(sr);
            dbConn = iPhoenixConn.getConn(url, properties);
        }

        return dbConn;
    }

    /**
     * 根据字段类型获取Phoenix转换实例
     * phoenix支持以下数据类型
     * @param type
     * @return
     */
    public static PDataType getPDataType(String type){
        if(StringUtils.isBlank(type)){
            throw new RuntimeException("type[" + type + "] cannot be blank!");
        }
        switch (type.toUpperCase()){
            case "INTEGER" :            return PInteger.INSTANCE;
            case "UNSIGNED_INT" :       return PUnsignedInt.INSTANCE;
            case "BIGINT" :             return PLong.INSTANCE;
            case "UNSIGNED_LONG" :      return PUnsignedLong.INSTANCE;
            case "TINYINT" :            return PTinyint.INSTANCE;
            case "UNSIGNED_TINYINT" :   return PUnsignedTinyint.INSTANCE;
            case "SMALLINT" :           return PSmallint.INSTANCE;
            case "UNSIGNED_SMALLINT" :  return PUnsignedSmallint.INSTANCE;
            case "FLOAT" :              return PFloat.INSTANCE;
            case "UNSIGNED_FLOAT" :     return PUnsignedFloat.INSTANCE;
            case "DOUBLE" :             return PDouble.INSTANCE;
            case "UNSIGNED_DOUBLE" :    return PUnsignedDouble.INSTANCE;
            case "DECIMAL" :            return PDecimal.INSTANCE;
            case "BOOLEAN" :            return PBoolean.INSTANCE;
            case "TIME" :               return PTime.INSTANCE;
            case "DATE" :               return PDate.INSTANCE;
            case "TIMESTAMP" :          return PTimestamp.INSTANCE;
            case "UNSIGNED_TIME" :      return PUnsignedTime.INSTANCE;
            case "UNSIGNED_DATE" :      return PUnsignedDate.INSTANCE;
            case "UNSIGNED_TIMESTAMP" : return PUnsignedTimestamp.INSTANCE;
            case "VARCHAR" :            return PVarchar.INSTANCE;
            case "CHAR" :               return PChar.INSTANCE;
            //不支持二进制字段类型
            case "BINARY" :             throw new RuntimeException("type [BINARY] is unsupported!");
            case "VARBINARY" :          throw new RuntimeException("type [VARBINARY] is unsupported!");
            default:                    throw new RuntimeException("type["+ type +"] is unsupported!");
        }
    }

    public static List<String> analyzeTable(ResultSet rs, List<MetaColumn> metaColumns) throws SQLException {
        List<String> ret = new ArrayList<>(metaColumns.size());
        ResultSetMetaData rd = rs.getMetaData();

        Map<String,String> nameTypeMap = new HashMap<>((rd.getColumnCount() << 2) / 3);
        for(int i = 0; i < rd.getColumnCount(); ++i) {
            nameTypeMap.put(rd.getColumnName(i+1),rd.getColumnTypeName(i+1));
        }

        for (MetaColumn metaColumn : metaColumns) {
            if(metaColumn.getValue() != null){
                ret.add("string");
            } else {
                ret.add(nameTypeMap.get(metaColumn.getName()));
            }
        }
        return ret;
    }

    public interface IPhoenixConn {
        default Connection getConn(String url, Properties properties) throws SQLException {
            throw new NotSupportedException("this method must be override");
        }
    }
}
