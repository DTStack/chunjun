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

package com.dtstack.flinkx.connector.doris.converter;

import com.dtstack.flinkx.connector.doris.options.DorisConf;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.ISerializationConverter;
import com.dtstack.flinkx.element.ColumnRowData;

import org.apache.flink.table.data.RowData;

import java.util.List;
import java.util.StringJoiner;

/**
 * Company：www.dtstack.com.
 *
 * @author shitou
 * @date 2021/11/10
 */
public class DorisColumnConverter
        extends AbstractRowConverter<RowData, RowData, StringJoiner, String> {

    private List<String> fullColumn;
    private List<String> columnNames;
    private final DorisConf options;

    private static final String NULL_VALUE = "\\N";

    public DorisColumnConverter(DorisConf options) {
        super(options.getColumn().size());
        this.options = options;
        for (int i = 0; i < options.getColumn().size(); i++) {
            toExternalConverters.add(
                    wrapIntoNullableExternalConverter(createExternalConverter(""), ""));
        }
    }

    @Override
    public RowData toInternal(RowData input) {
        return null;
    }

    @Override
    public StringJoiner toExternal(RowData rowData, StringJoiner joiner) throws Exception {
        if (fullColumn.size() == options.getColumn().size()) {
            for (int index = 0; index < rowData.getArity(); index++) {
                toExternalConverters.get(index).serialize(rowData, index, joiner);
            }
        } else {
            for (String columnName : fullColumn) {
                if (columnNames.contains(columnName)) {
                    int index = columnNames.indexOf(columnName);
                    toExternalConverters.get(index).serialize(rowData, index, joiner);
                } else {
                    joiner.add(NULL_VALUE);
                }
            }
        }
        return joiner;
    }

    @Override
    protected ISerializationConverter<StringJoiner> wrapIntoNullableExternalConverter(
            ISerializationConverter<StringJoiner> ISerializationConverter, String type) {
        return ((rowData, index, joiner) -> {
            if (rowData == null || rowData.isNullAt(index)) {
                joiner.add(NULL_VALUE);
            } else {
                ISerializationConverter.serialize(rowData, index, joiner);
            }
        });
    }

    @Override
    protected ISerializationConverter<StringJoiner> createExternalConverter(String type) {
        return (rowData, index, joiner) -> {
            Object value = ((ColumnRowData) rowData).getField(index);
            joiner.add("".equals(value.toString()) ? NULL_VALUE : value.toString());
        };
    }

    public void setFullColumn(List<String> fullColumn) {
        this.fullColumn = fullColumn;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }
}
