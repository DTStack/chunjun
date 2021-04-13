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
package com.dtstack.flinkx.pgwal;

import com.dtstack.flinkx.reader.MetaColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Date: 2019/12/14
 * Company: www.dtstack.com
 *
 * reference to https://github.com/debezium/debezium & http://www.postgres.cn/docs/10/protocol-logicalrep-message-formats.html
 *
 * @author tudou
 */
public class PgDecoder {
    private static final Logger LOG = LoggerFactory.getLogger(PgDecoder.class);

    private static Instant PG_EPOCH = LocalDate.of(2000, 1, 1).atStartOfDay().toInstant(ZoneOffset.UTC);

    private Map<Integer, Table> tableMap = new HashMap<>(64);
    private Map<Integer, String> pgTypeMap;
    private volatile long currentLsn;
    private volatile long ts;

    public PgDecoder(Map<Integer, String> pgTypeMap) {
        this.pgTypeMap = pgTypeMap;
    }

    private static String readColumnValueAsString(ByteBuffer buffer) {
        //Int32 列值的长度
        int length = buffer.getInt();
        byte[] value = new byte[length];
        //Byte(n) 该列的值，以文本格式显示。n是上面的长度
        buffer.get(value, 0, length);
        return new String(value);
    }

    private static String readString(ByteBuffer buffer) {
        StringBuilder sb = new StringBuilder();
        byte b = 0;
        while ((b = buffer.get()) != 0) {
            sb.append((char) b);
        }
        return sb.toString();
    }

    public static String unquoteIdentifierPart(String identifierPart) {
        if (identifierPart == null || identifierPart.length() < 2) {
            return identifierPart;
        }

        Character quotingChar = deriveQuotingChar(identifierPart);
        if (quotingChar != null) {
            identifierPart = identifierPart.substring(1, identifierPart.length() - 1);
            identifierPart = identifierPart.replace(quotingChar.toString() + quotingChar.toString(), quotingChar.toString());
        }

        return identifierPart;
    }

    private static Character deriveQuotingChar(String identifierPart) {
        char first = identifierPart.charAt(0);
        char last = identifierPart.charAt(identifierPart.length() - 1);

        if (first == last && (first == '"' || first == '\'' || first == '`')) {
            return first;
        }

        return null;
    }

    public Table decode(ByteBuffer buffer) throws SQLException {
        Table table = new Table();
        PgMessageTypeEnum type = PgMessageTypeEnum.forType((char) buffer.get());
        switch (type) {
            case BEGIN:
                //Byte1('B') 将消息标识为开始消息
                handleBeginMessage(buffer);
                break;
            case COMMIT:
                //Byte1('C') 将消息标识为提交消息
                handleCommitMessage(buffer);
                break;
            case RELATION:
                //Byte1('R') 将消息标识为关系消息
                handleRelationMessage(buffer);
                break;
            case INSERT:
                //Byte1('I') 将消息标识为插入消息
                table = decodeInsert(buffer);
                break;
            case UPDATE:
                //Byte1('U') 将消息标识为更新消息
                table = decodeUpdate(buffer);
                break;
            case DELETE:
                //Byte1('D') 将消息标识为删除消息
                table = decodeDelete(buffer);
                break;
            default:
                break;
        }
        table.setType(type);
        return table;
    }

    private void handleBeginMessage(ByteBuffer buffer) {
        //Int64 事务的结束LSN
        long lsn = buffer.getLong();
        //Int64 提交事务的时间戳。自PostgreSQL纪元（2000-01-01）以来的数值是微秒数
        Instant plus = PG_EPOCH.plus(buffer.getLong(), ChronoUnit.MICROS);
        //Int32 事务的Xid
        int anInt = buffer.getInt();
        currentLsn = lsn;
        ts = plus.toEpochMilli();
        LOG.trace("handleBeginMessage result = { lsn = {}, plus = {}, anInt = {}}", lsn, plus, anInt);
    }

    private void handleCommitMessage(ByteBuffer buffer) {
        if(LOG.isTraceEnabled()){
            //Int8 标志；目前未使用（必须为0）
            int flags = buffer.get();
            //Int64 提交的LSN
            long lsn = buffer.getLong();
            //Int64 事务的结束LSN
            long endLsn = buffer.getLong();
            //Int64 提交事务的时间戳。自PostgreSQL纪元（2000-01-01）以来的数值是微秒数
            Instant commitTimestamp = PG_EPOCH.plus(buffer.getLong(), ChronoUnit.MICROS);
            LOG.trace("handleCommitMessage result = { flags = {}, lsn = {}, endLsn = {}, commitTimestamp = {}}", flags, lsn, endLsn, commitTimestamp);
        }
    }

    private void handleRelationMessage(ByteBuffer buffer) throws SQLException {
        //Int32 关系的ID
        int relationId = buffer.getInt();
        //String 命名空间（pg_catalog的空字符串）
        String schemaName = readString(buffer);
        //String 关系名称
        String tableName = readString(buffer);
        //Int8 该关系的副本标识设置（与pg_class 中的relreplident相同）
        int replicaIdentityId = buffer.get();
        //Int16 列数
        short columnCount = buffer.getShort();
        LOG.debug("handleRelationMessage result = { schemaName = {}, tableName = {}}", schemaName, tableName);
        if(!tableMap.containsKey(relationId)){
            List<MetaColumn> columnList = new ArrayList<>(columnCount);
            for (int i = 0; i < columnCount; i++) {
                //Int8 列的标志。当前可以是0表示没有标记或1表示将列标记为关键字的一部分
                byte flags = buffer.get();
                //String 列的名称
                String name = unquoteIdentifierPart(readString(buffer));
                //Int32 列的数据类型的ID
                String type = pgTypeMap.get(buffer.getInt());
                MetaColumn metaColumn = new MetaColumn();
                metaColumn.setIndex(i);
                metaColumn.setName(name);
                metaColumn.setType(type);
                columnList.add(metaColumn);
                //Int32 列的类型修饰符(atttypmod)
                int attypmod = buffer.getInt();
            }
            Table table = new Table(schemaName, tableName, columnList);
            tableMap.put(relationId, table);
        }
    }

    private Table decodeInsert(ByteBuffer buffer) {
        //Int32 与关系消息中的ID对应的关系的ID
        int relationId = buffer.getInt();
        //Byte1('N') 将以下TupleData消息标识为新元组
        char tupleType = (char) buffer.get();
        //TupleData TupleData消息部分表示新元组的内容
        Object[] newData = resolveColumnsFromStreamTupleData(buffer);
        Table table = tableMap.get(relationId);
        table.setOldData(new Object[newData.length]);
        table.setNewData(newData);
        table.setCurrentLsn(currentLsn);
        table.setTs(ts);
        return table;
    }

    private Table decodeUpdate(ByteBuffer buffer) throws SQLException {
        //Int32 与关系消息中的ID对应的关系的ID
        int relationId = buffer.getInt();
        Table table = tableMap.get(relationId);
        //Byte1('K') 将以下TupleData子消息标识为键。该字段是可选的， 并且只有在更新改变了REPLICA IDENTITY索引一部分的任何一列中的数据时才存在
        //Byte1('O') 将以下TupleData子消息标识为旧元组。此字段是可选的， 并且仅当发生更新的表的REPLICA IDENTITY设置为FULL时才存在
        //更新消息可以包含'K'消息部分或者'O'消息部分或者都不包含它们，但不同时包括它们两者
        char tupleType = (char) buffer.get();
        if ('O' == tupleType || 'K' == tupleType) {
            //TupleData TupleData消息部分表示旧元组或主键的内容。 只有在前面的'O'或'K'部分存在时才存在
            Object[] oldData = resolveColumnsFromStreamTupleData(buffer);
            table.setOldData(oldData);
            // Read the 'N' tuple type
            // This is necessary so the stream position is accurate for resolving the column tuple data
            //Byte1('N') 将以下TupleData消息标识为新元组
            tupleType = (char) buffer.get();
        }
        //TupleData TupleData消息部分表示新元组的内容
        Object[] newData = resolveColumnsFromStreamTupleData(buffer);
        table.setNewData(newData);
        table.setCurrentLsn(currentLsn);
        table.setTs(ts);
        return table;
    }

    private Table decodeDelete(ByteBuffer buffer) throws SQLException {
        //Int32 与关系消息中的ID对应的关系的ID
        int relationId = buffer.getInt();
        Table table = tableMap.get(relationId);
        //Byte1('K') 将以下TupleData子消息标识为键。 如果发生删除的表使用索引作为REPLICA IDENTITY，则此字段存在
        //Byte1('O') 将以下TupleData消息标识为旧元组。 如果发生删除的表的REPLICA IDENTITY设置为FULL，则此字段存在
        //删除消息可能包含'K'消息部分或'O'消息部分，但不会同时包含这两个部分
        char tupleType = (char) buffer.get();
        //TupleData TupleData消息部分，表示旧元组或主键的内容，具体取决于前一个字段
        Object[] oldData = resolveColumnsFromStreamTupleData(buffer);
        table.setOldData(oldData);
        table.setNewData(new Object[oldData.length]);
        table.setCurrentLsn(currentLsn);
        table.setTs(ts);
        return table;
    }

    private Object[] resolveColumnsFromStreamTupleData(ByteBuffer buffer) {
        //Int16 列数
        short numberOfColumns = buffer.getShort();
        Object[] data = new Object[numberOfColumns];
        for (int i = 0; i < numberOfColumns; i++) {

            //Byte1('n') 将数据标识为NULL值
            //Byte1('u') 识别未更改的TOASTed值（实际值未发送）
            //Byte1('t') 将数据标识为文本格式的值
            char type = (char) buffer.get();
            if (type == 't') {
                data[i] = readColumnValueAsString(buffer);
            } else if (type == 'u') {
                data[i] = null;
            } else if (type == 'n') {
                data[i] = null;
            }
        }
        return data;
    }


}
