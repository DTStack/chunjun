/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.connector.oracle.source;

import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormat;
import com.dtstack.chunjun.connector.oracle.converter.BlobType;
import com.dtstack.chunjun.connector.oracle.converter.ClobType;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import oracle.jdbc.OracleStatement;
import oracle.jdbc.OracleTypes;

import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;

/**
 * company www.dtstack.com
 *
 * @author jier
 */
public class OracleInputFormat extends JdbcInputFormat {

    @Override
    protected void defineColumnType(Statement statement) throws SQLException {
        if (statement instanceof oracle.jdbc.OracleStatement) {
            OracleStatement oracleStatement = (OracleStatement) statement;
            ArrayList<String> names = new ArrayList<>();
            ArrayList<String> types = new ArrayList<>();
            jdbcConf.getColumn()
                    .forEach(
                            i -> {
                                names.add(i.getName());
                                types.add(i.getType());
                            });
            RowType rowType =
                    TableUtil.createRowType(names, types, jdbcDialect.getRawTypeConverter());
            OracleStatement oraclePreparedStatement = (OracleStatement) statement;
            oraclePreparedStatement.clearDefines();
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                defineType(i, rowType.getTypeAt(i), oracleStatement);
            }
        }
    }

    protected void defineType(int position, LogicalType logicalType, OracleStatement statement)
            throws SQLException {
        switch (logicalType.getTypeRoot()) {
            case SMALLINT:
            case INTEGER:
            case FLOAT:
            case DOUBLE:
            case BIGINT:
            case DECIMAL:
                statement.defineColumnType(position + 1, Types.VARCHAR);
                break;
            case CHAR:
            case VARCHAR:
                if (!(logicalType instanceof ClobType)) {
                    statement.defineColumnType(position + 1, Types.VARCHAR);
                }
                break;
            case DATE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                statement.defineColumnType(position + 1, Types.TIMESTAMP);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                statement.defineColumnType(position + 1, OracleTypes.TIMESTAMPLTZ);
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
                statement.defineColumnType(position + 1, OracleTypes.TIMESTAMPTZ);
                break;
            case BINARY:
            case VARBINARY:
                if (!(logicalType instanceof BlobType)) {
                    statement.defineColumnType(position + 1, Types.VARBINARY);
                }
                break;
        }
    }
}
