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
package com.dtstack.flinkx.connector.kingbase.sink;

import org.apache.flink.table.types.logical.RowType;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.jdbc.sink.JdbcOutputFormat;
import com.dtstack.flinkx.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.flinkx.connector.jdbc.util.JdbcUtil;
import com.dtstack.flinkx.connector.kingbase.converter.KingbaseRawTypeConverter;
import com.dtstack.flinkx.connector.kingbase.util.KingbaseUtils;
import com.dtstack.flinkx.enums.EWriteMode;
import com.dtstack.flinkx.util.JsonUtil;
import com.dtstack.flinkx.util.TableUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @description:
 * @program: flinkx-all
 * @author: lany
 * @create: 2021/05/13 20:10
 */
public class KingbaseOutputFormat extends JdbcOutputFormat {

    protected static final long serialVersionUID = 2L;

    /**
     * override reason: The Kingbase meta-database is case sensitive。
     * If you use a lowercase table name, it will not be able to query the table metadata.
     * so we convert the table and schema name to uppercase.
     *
     * @param taskNumber
     * @param numTasks
     */
    @Override
    protected void openInternal(int taskNumber, int numTasks) {

        try {
            dbConn = getConnection();

            //默认关闭事务自动提交，手动控制事务
            dbConn.setAutoCommit(false);

            Pair<List<String>, List<String>> pair = KingbaseUtils.getTableMetaData(
                    jdbcConf.getSchema(),
                    jdbcConf.getTable(),
                    dbConn);
            List<String> fullColumn = pair.getLeft();
            List<String> fullColumnType = pair.getRight();

            List<FieldConf> fieldList = jdbcConf.getColumn();
            if (fieldList.size() == 1 && Objects.equals(fieldList.get(0).getName(), "*")) {
                column = fullColumn;
                columnType = fullColumnType;
            } else {
                column = new ArrayList<>(fieldList.size());
                columnType = new ArrayList<>(fieldList.size());
                for (FieldConf fieldConf : fieldList) {
                    column.add(fieldConf.getName());
                    for (int i = 0; i < fullColumn.size(); i++) {
                        if (fieldConf.getName().equalsIgnoreCase(fullColumn.get(i))) {
                            columnType.add(fullColumnType.get(i));
                            break;
                        }
                    }
                }
            }

            if (!EWriteMode.INSERT.name().equalsIgnoreCase(jdbcConf.getMode())) {
                List<String> updateKey = jdbcConf.getUpdateKey();
                if (CollectionUtils.isEmpty(updateKey)) {
                    List<String> tableIndex =
                            JdbcUtil.getTableIndex(
                                    jdbcConf.getSchema(), jdbcConf.getTable(), dbConn);
                    jdbcConf.setUpdateKey(tableIndex);
                    LOG.info("updateKey = {}", JsonUtil.toPrintJson(tableIndex));
                }
            }


            fieldNamedPreparedStatement = FieldNamedPreparedStatement.prepareStatement(
                    dbConn,
                    prepareTemplates(),
                    this.column.toArray(new String[0]));

            LOG.info("subTask[{}}] wait finished", taskNumber);
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        } finally {
            JdbcUtil.commit(dbConn);
        }

        // create row converter
        RowType rowType =
                TableUtil.createRowType(column, columnType, KingbaseRawTypeConverter::apply);
        setRowConverter(jdbcDialect.getColumnConverter(rowType));
    }
}
