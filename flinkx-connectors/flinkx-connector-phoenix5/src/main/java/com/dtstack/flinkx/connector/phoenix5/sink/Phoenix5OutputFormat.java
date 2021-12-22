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

package com.dtstack.flinkx.connector.phoenix5.sink;

import com.dtstack.flinkx.connector.jdbc.sink.JdbcOutputFormat;
import com.dtstack.flinkx.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.flinkx.connector.jdbc.util.JdbcUtil;
import com.dtstack.flinkx.connector.phoenix5.converter.Phoenix5RawTypeConverter;
import com.dtstack.flinkx.connector.phoenix5.util.Phoenix5Util;
import com.dtstack.flinkx.enums.EWriteMode;
import com.dtstack.flinkx.enums.Semantic;
import com.dtstack.flinkx.util.TableUtil;

import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.lang3.tuple.Pair;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @author wujuan
 * @version 1.0
 * @date 2021/7/9 16:01 星期五
 * @email wujuan@dtstack.com
 * @company www.dtstack.com
 */
public class Phoenix5OutputFormat extends JdbcOutputFormat {

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        jdbcConf.setMode(EWriteMode.UPSERT.name());
        try {
            dbConn = getConnection();
            // Turn off automatic transaction commit by default, and manually control the
            // transaction.
            if (Semantic.EXACTLY_ONCE == semantic) {
                dbConn.setAutoCommit(false);
            }
            initColumnList();

            LOG.info("subTask[{}] wait finished", taskNumber);
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        } finally {
            if (Semantic.EXACTLY_ONCE == semantic) {
                JdbcUtil.commit(dbConn);
            }
        }

        RowType rowType =
                TableUtil.createRowType(
                        columnNameList, columnTypeList, Phoenix5RawTypeConverter::apply);
        setRowConverter(
                null == rowConverter ? jdbcDialect.getColumnConverter(rowType) : rowConverter);
    }

    @Override
    protected Pair<List<String>, List<String>> getTableMetaData() {
        // dbConn = getConnection();
        return Phoenix5Util.getTableMetaData(
                jdbcConf.getColumn(), jdbcConf.getTable(), getConnection());
    }

    @Override
    protected String prepareTemplates() {
        String singleSql;
        if (EWriteMode.UPSERT.name().equalsIgnoreCase(jdbcConf.getMode())) {
            singleSql =
                    jdbcDialect
                            .getUpsertStatement(
                                    jdbcConf.getSchema(),
                                    jdbcConf.getTable(),
                                    columnNameList.toArray(new String[0]),
                                    jdbcConf.getUpdateKey() != null
                                            ? jdbcConf.getUpdateKey().toArray(new String[0])
                                            : null,
                                    jdbcConf.isAllReplace())
                            .get();
        } else {
            throw new IllegalArgumentException("Phoenix5 unknown write mode:" + jdbcConf.getMode());
        }

        LOG.info("write sql:{}", singleSql);
        return singleSql;
    }

    @Override
    protected Connection getConnection() {
        Connection conn =
                Phoenix5Util.getConnection(jdbcDialect.defaultDriverName().get(), jdbcConf);

        return conn;
    }
}
