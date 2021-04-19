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

package com.dtstack.flinkx.connector.mysql.inputFormat;

import com.dtstack.flinkx.RawTypeConverter;
import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.mysql.converter.MysqlTypeConverter;

import com.dtstack.flinkx.util.TableTypeUtils;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import com.dtstack.flinkx.connector.jdbc.inputFormat.JdbcInputFormat;

import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

/**
 * Date: 2021/04/12
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class MysqlInputFormat extends JdbcInputFormat {

    @Override
    public void openInternal(InputSplit inputSplit) {
        super.openInternal(inputSplit);
        try {
            LogicalType rowType = TableTypeUtils.createRowType(rawFieldNames, rawFieldTypes, MysqlTypeConverter::apply);
            setRowConverter(jdbcDialect.getRowConverter((RowType) rowType));
        } catch (SQLException e) {
            LOG.error("", e);
        }
    }

    @Override
    public RowData nextRecordInternal(RowData rowData) throws IOException {
        if (!hasNext) {
            return null;
        }

        try {
            // TODO 如果没常量可以不调用
            GenericRowData rawRowData = (GenericRowData) jdbcRowConverter.toInternal(resultSet);
            GenericRowData finalRowData = loadConstantData(rawRowData);
            return super.nextRecordInternal(finalRowData);
        }catch (Exception e) {
            throw new IOException("Couldn't read data - " + e.getMessage(), e);
        }
    }

    /**
     * 填充常量
     * @param rawRowData
     * @return
     */
    protected GenericRowData loadConstantData(GenericRowData rawRowData) {
        int len = finalFieldTypes.size();
        List<FieldConf> fieldConfs = jdbcConf.getColumn();
        GenericRowData finalRowData = new GenericRowData(len);
        for (int i = 0; i < len; i++) {
            String val = fieldConfs.get(i).getValue();
            if (val != null) {
                finalRowData.setField(i, StringData.fromString(val));
            } else {
                finalRowData.setField(i, rawRowData.getField(i));
            }
        }
        return finalRowData;
    }

    @Override
    public LogicalType getLogicalType() throws SQLException {
        return super.getLogicalType(MysqlTypeConverter::apply);
    }
}
