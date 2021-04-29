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

package com.dtstack.flinkx.connector.mysql.source;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.element.ColumnRowData;
import com.dtstack.flinkx.element.column.StringColumn;
import com.dtstack.flinkx.util.StringUtil;

import org.apache.flink.core.io.InputSplit;

import com.dtstack.flinkx.connector.jdbc.source.JdbcInputFormat;

import org.apache.flink.table.data.RowData;

import java.util.List;

/**
 * Date: 2021/04/12 Company: www.dtstack.com
 *
 * @author tudou
 */
public class MysqlInputFormat extends JdbcInputFormat {

    @Override
    public void openInternal(InputSplit inputSplit) {
        super.openInternal(inputSplit);
        setRowConverter(jdbcDialect.getRowConverter(columnTypeList));
    }

    /**
     * 填充常量 { "name": "raw_date", "type": "string", "value": "2014-12-12 14:24:16" }
     *
     * @param rawRowData
     * @return
     */
    @Override
    protected RowData loadConstantData(RowData rawRowData) {
        int len = finalFieldTypes.size();
        List<FieldConf> fieldConfs = jdbcConf.getColumn();
        ColumnRowData finalRowData = new ColumnRowData(len);
        for (int i = 0; i < len; i++) {
            String val = fieldConfs.get(i).getValue();
            // 代表设置了常量即value有值，不管数据库中有没有对应字段的数据，用json中的值替代
            if (val != null) {
                String value =
                        StringUtil.string2col(
                                val,
                                fieldConfs.get(i).getType(),
                                fieldConfs.get(i).getTimeFormat()).toString();
                finalRowData.addField(new StringColumn(value));
            } else {
                finalRowData.addField(((ColumnRowData) rawRowData).getField(i));
            }
        }
        return finalRowData;
    }
}
