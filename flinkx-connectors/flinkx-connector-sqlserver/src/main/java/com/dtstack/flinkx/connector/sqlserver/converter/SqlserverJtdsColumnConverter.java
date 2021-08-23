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

package com.dtstack.flinkx.connector.sqlserver.converter;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.conf.FlinkxCommonConf;
import com.dtstack.flinkx.connector.jdbc.converter.JdbcColumnConverter;
import com.dtstack.flinkx.element.AbstractBaseColumn;
import com.dtstack.flinkx.element.ColumnRowData;
import com.dtstack.flinkx.element.column.BigDecimalColumn;
import com.dtstack.flinkx.util.StringUtil;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.util.List;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/8/12 11:24
 */
public class SqlserverJtdsColumnConverter extends JdbcColumnConverter {

    public SqlserverJtdsColumnConverter(RowType rowType, FlinkxCommonConf commonConf) {
        super(rowType, commonConf);
    }

    @Override
    @SuppressWarnings("unchecked")
    public RowData toInternal(ResultSet resultSet) throws Exception {
        List<FieldConf> fieldConfList = commonConf.getColumn();
        ColumnRowData result = new ColumnRowData(fieldConfList.size());
        int converterIndex = 0;
        for (FieldConf fieldConf : fieldConfList) {
            AbstractBaseColumn baseColumn = null;
            if (StringUtils.isBlank(fieldConf.getValue())) {
                Object field = resultSet.getObject(converterIndex + 1);
                // in sqlserver, timestamp type is a binary array of 8 bytes.
                if ("timestamp".equalsIgnoreCase(fieldConf.getType())) {
                    byte[] value = (byte[]) field;
                    String hexString = StringUtil.bytesToHexString(value);
                    baseColumn = new BigDecimalColumn(Long.parseLong(hexString, 16));
                } else {
                    baseColumn =
                            (AbstractBaseColumn)
                                    toInternalConverters.get(converterIndex).deserialize(field);
                }
                converterIndex++;
            }
            result.addField(assembleFieldProps(fieldConf, baseColumn));
        }
        return result;
    }
}
