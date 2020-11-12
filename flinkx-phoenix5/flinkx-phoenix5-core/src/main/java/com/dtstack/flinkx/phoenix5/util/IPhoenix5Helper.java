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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Properties;

/**
 * Date: 2020/10/10
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public interface IPhoenix5Helper {

    String CLASS_STR = "public transient RowProjector rowProjector;\n" +
            "    public List<PDataType> instanceList;\n" +
            "\n" +
            "    @Override\n" +
            "    public Connection getConn(String url, Properties properties) throws SQLException {\n" +
            "        Connection dbConn;\n" +
            "        synchronized (ClassUtil.LOCK_STR) {\n" +
            "            DriverManager.setLoginTimeout(10);\n" +
            "            // telnet\n" +
            "            TelnetUtil.telnet(url);\n" +
            "            dbConn = DriverManager.getConnection(url, properties);\n" +
            "        }\n" +
            "\n" +
            "        return dbConn;\n" +
            "    }\n" +
            "\n" +
            "    @Override\n" +
            "    public List<Pair<byte[], byte[]>> getRangeList(PreparedStatement ps) {\n" +
            "        List<KeyRange> rangeList = ((PhoenixPreparedStatement) ps).getQueryPlan().getSplits();\n" +
            "        List<Pair<byte[], byte[]>> list = new ArrayList(rangeList.size());\n" +
            "        for (KeyRange keyRange : rangeList) {\n" +
            "            list.add(Pair.of(keyRange.getLowerRange(), keyRange.getUpperRange()));\n" +
            "        }\n" +
            "        return list;\n" +
            "    }\n" +
            "\n" +
            "    @Override\n" +
            "    public Map<byte[], NavigableSet<byte[]>> getFamilyMap(ResultSet resultSet) throws Exception {\n" +
            "        Field field = PhoenixResultSet.class.getDeclaredField(\"rowProjector\");\n" +
            "        field.setAccessible(true);\n" +
            "        rowProjector = (RowProjector) field.get(resultSet);\n" +
            "        field.setAccessible(false);\n" +
            "        Field c = PhoenixResultSet.class.getDeclaredField(\"context\");\n" +
            "        c.setAccessible(true);\n" +
            "        StatementContext context = (StatementContext)c.get(resultSet);\n" +
            "        c.setAccessible(false);\n" +
            "        return context.getScan().getFamilyMap();\n" +
            "    }\n" +
            "\n" +
            "    @Override\n" +
            "    public void initInstanceList(List<String> typeList) {\n" +
            "        instanceList = new ArrayList(typeList.size());\n" +
            "        for (String type : typeList) {\n" +
            "            instanceList.add(getPDataType(type));\n" +
            "        }\n" +
            "    }\n" +
            "\n" +
            "\n" +
            "    @Override\n" +
            "    public byte[] getScanProjector(ResultSet resultSet) {\n" +
            "        StatementContext context = ((PhoenixResultSet)resultSet).getContext();\n" +
            "        return context.getScan().getAttribute(\"scanProjector\");\n" +
            "    }\n" +
            "\n" +
            "    @Override\n" +
            "    public Row getRow(byte[] bytes, int offset, int length) throws SQLException {\n" +
            "        int size = instanceList.size();\n" +
            "        Row row = new Row(size);\n" +
            "        ImmutableBytesWritable pointer = new ImmutableBytesWritable();\n" +
            "        for (int pos = 0; pos < size; pos++) {\n" +
            "            PDataType pDataType = (PDataType)instanceList.get(pos);\n" +
            "            row.setField(pos, rowProjector.getColumnProjector(pos).getValue(new ResultTuple(Result.create(Collections.singletonList(new NoTagsKeyValue(bytes, offset, length)))), pDataType, pointer));\n" +
            "        }\n" +
            "        return row;\n" +
            "    }\n" +
            "\n" +
            "    @Override\n" +
            "    public Map<String, Object> analyzePhoenixUrl(String url) throws SQLException {\n" +
            "        PhoenixEmbeddedDriver.ConnectionInfo info = PhoenixEmbeddedDriver.ConnectionInfo.create(url);\n" +
            "        Map<String, Object> map = new HashMap(8);\n" +
            "        //zk地址\n" +
            "        map.put(\"quorum\", info.getZookeeperQuorum());\n" +
            "        //zk端口\n" +
            "        map.put(\"port\", info.getPort());\n" +
            "        //hbase zk节点名称\n" +
            "        map.put(\"rootNode\", info.getRootNode());\n" +
            "        map.put(\"principal\", info.getPrincipal());\n" +
            "        map.put(\"keytabFile\", info.getKeytab());\n" +
            "        return map;\n" +
            "    }\n" +
            "\n" +
            "    /**\n" +
            "     * 根据字段类型获取Phoenix转换实例\n" +
            "     * phoenix支持以下数据类型\n" +
            "     * @param type\n" +
            "     * @return\n" +
            "     */\n" +
            "    public PDataType getPDataType(String type){\n" +
            "        if(StringUtils.isBlank(type)){\n" +
            "            throw new RuntimeException(\"type[\" + type + \"] cannot be blank!\");\n" +
            "        }\n" +
            "        switch (type.toUpperCase()){\n" +
            "            case \"INTEGER\" :            return PInteger.INSTANCE;\n" +
            "            case \"UNSIGNED_INT\" :       return PUnsignedInt.INSTANCE;\n" +
            "            case \"BIGINT\" :             return PLong.INSTANCE;\n" +
            "            case \"UNSIGNED_LONG\" :      return PUnsignedLong.INSTANCE;\n" +
            "            case \"TINYINT\" :            return PTinyint.INSTANCE;\n" +
            "            case \"UNSIGNED_TINYINT\" :   return PUnsignedTinyint.INSTANCE;\n" +
            "            case \"SMALLINT\" :           return PSmallint.INSTANCE;\n" +
            "            case \"UNSIGNED_SMALLINT\" :  return PUnsignedSmallint.INSTANCE;\n" +
            "            case \"FLOAT\" :              return PFloat.INSTANCE;\n" +
            "            case \"UNSIGNED_FLOAT\" :     return PUnsignedFloat.INSTANCE;\n" +
            "            case \"DOUBLE\" :             return PDouble.INSTANCE;\n" +
            "            case \"UNSIGNED_DOUBLE\" :    return PUnsignedDouble.INSTANCE;\n" +
            "            case \"DECIMAL\" :            return PDecimal.INSTANCE;\n" +
            "            case \"BOOLEAN\" :            return PBoolean.INSTANCE;\n" +
            "            case \"TIME\" :               return PTime.INSTANCE;\n" +
            "            case \"DATE\" :               return PDate.INSTANCE;\n" +
            "            case \"TIMESTAMP\" :          return PTimestamp.INSTANCE;\n" +
            "            case \"UNSIGNED_TIME\" :      return PUnsignedTime.INSTANCE;\n" +
            "            case \"UNSIGNED_DATE\" :      return PUnsignedDate.INSTANCE;\n" +
            "            case \"UNSIGNED_TIMESTAMP\" : return PUnsignedTimestamp.INSTANCE;\n" +
            "            case \"VARCHAR\" :            return PVarchar.INSTANCE;\n" +
            "            case \"CHAR\" :               return PChar.INSTANCE;\n" +
            "            //不支持二进制字段类型\n" +
            "            case \"BINARY\" :             throw new RuntimeException(\"type [BINARY] is unsupported!\");\n" +
            "            case \"VARBINARY\" :          throw new RuntimeException(\"type [VARBINARY] is unsupported!\");\n" +
            "            default:                    throw new RuntimeException(\"type[\"+ type +\"] is unsupported!\");\n" +
            "        }\n" +
            "    }";

    /**
     * 获取phoenix jdbc连接
     * @param url
     * @param properties
     * @return
     * @throws SQLException
     */
    Connection getConn(String url, Properties properties) throws SQLException;

    /**
     * 获取region边界
     * @param ps
     * @return
     */
    List<Pair<byte[], byte[]>> getRangeList(PreparedStatement ps);

    /**
     * 获取HBase列族
     * @param resultSet
     * @return
     */
    Map<byte [], NavigableSet<byte []>> getFamilyMap(ResultSet resultSet) throws Exception;

    /**
     * 初始化Phoenix数据格式实例
     * @param typeList
     */
    void initInstanceList(List<String> typeList);

    /**
     * 获取ScanProjector属性
     * @param resultSet
     * @return
     * @throws Exception
     */
    byte [] getScanProjector(ResultSet resultSet);

    /**
     * 获取HBase值
     * @param bytes
     * @param offset
     * @param length
     * @return
     * @throws SQLException
     */
    Row getRow(byte[] bytes, int offset, int length) throws SQLException;

    /**
     * 解析Phoenix JDBC URL
     * @param url
     * @return
     * @throws SQLException
     */
    Map<String, Object> analyzePhoenixUrl(String url) throws SQLException;
}
