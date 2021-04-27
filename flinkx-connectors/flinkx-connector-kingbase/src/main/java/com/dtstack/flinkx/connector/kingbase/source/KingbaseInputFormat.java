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
package com.dtstack.flinkx.connector.kingbase.source;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.types.logical.RowType;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.jdbc.source.JdbcInputFormat;
import com.dtstack.flinkx.connector.kingbase.converter.KingbaseRawTypeConverter;
import com.dtstack.flinkx.connector.kingbase.util.KingbaseUtils;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.TableUtil;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @description:
 * @program: flinkx-all
 * @author: lany
 * @create: 2021/05/13 20:10
 */
public class KingbaseInputFormat extends JdbcInputFormat {

    @Override
    public void openInternal(InputSplit inputSplit) {
        super.openInternal(inputSplit);
        RowType rowType =
                TableUtil.createRowType(
                        column, columnType, KingbaseRawTypeConverter::apply);
        setRowConverter(jdbcDialect.getColumnConverter(rowType));
    }

    @Override
    protected void analyzeMetaData() {
        try {
            List<FieldConf> fieldList = jdbcConf.getColumn();

            Pair<List<String>, List<String>> pair = KingbaseUtils.getTableMetaData(jdbcConf.getSchema(), jdbcConf.getTable(), dbConn);
            List<String> fullColumn = pair.getLeft();
            List<String> fullColumnType = pair.getRight();

            column = new ArrayList<>(fieldList.size());
            columnType = new ArrayList<>(fieldList.size());
            for (FieldConf fieldConf : jdbcConf.getColumn()) {
                column.add(fieldConf.getName());
                for (int i = 0; i < fullColumn.size(); i++) {
                    if (fieldConf.getName().equalsIgnoreCase(fullColumn.get(i))) {
                        if (fieldConf.getValue() != null) {
                            columnType.add("VARCHAR");
                        } else {
                            columnType.add(fullColumnType.get(i));
                        }
                        break;
                    }
                }
            }
        } catch (SQLException e) {
            String message = String.format("error to analyzeSchema, resultSet = %s, finalFieldTypes = %s, e = %s",
                    resultSet,
                    GsonUtil.GSON.toJson(columnType),
                    ExceptionUtil.getErrorMessage(e));
            LOG.error(message);
            throw new RuntimeException(message);
        }
    }
}
