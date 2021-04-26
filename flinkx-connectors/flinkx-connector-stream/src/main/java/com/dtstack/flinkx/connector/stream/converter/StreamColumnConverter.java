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
package com.dtstack.flinkx.connector.stream.converter;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import com.dtstack.flinkx.element.AbstractBaseColumn;
import com.dtstack.flinkx.element.BigDecimalColumn;
import com.dtstack.flinkx.element.ColumnRowData;
import com.dtstack.flinkx.element.StringColumn;
import com.github.jsonzou.jmockdata.JMockData;

import java.math.BigDecimal;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Date: 2021/04/26
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class StreamColumnConverter extends StreamBaseConverter {
    private static final AtomicLong id = new AtomicLong(0L);
//    private final List<String> typeList;

    public StreamColumnConverter(List<String> typeList) {
        super.toInternalConverters = new DeserializationConverter[typeList.size()];
//        this.typeList = typeList;
        for (int i = 0; i < typeList.size(); i++) {
            toInternalConverters[i] = createInternalConverter(typeList.get(i));
        }
    }

    public StreamColumnConverter(){}

    protected DeserializationConverter createInternalConverter(String type) {
        switch (type.toLowerCase(Locale.ENGLISH)) {
            case "id":
                return val -> {
                    BigDecimal bigDecimal = new BigDecimal(id.incrementAndGet());
                    return new BigDecimalColumn(bigDecimal);
                };
            case "int":
            case "integer":
                return val -> {
                    BigDecimal bigDecimal = new BigDecimal(JMockData.mock(int.class));
                    return new BigDecimalColumn(bigDecimal);
                };
            default:
                return val -> {
                    String string = JMockData.mock(String.class);
                    return new StringColumn(string);
                };
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public RowData toInternal(RowData rowData) throws Exception {
        ColumnRowData data = new ColumnRowData(toInternalConverters.length);
        for (int i = 0; i < toInternalConverters.length; i++) {
            data.addField((AbstractBaseColumn) toInternalConverters[i].deserialize(null));
        }
        return data;
    }

    @Override
    public RowData toExternal(RowData rowData, RowData output) {
        ColumnRowData genericRowData = (ColumnRowData) rowData;
        GenericRowData outputRowData = (GenericRowData) output;
        for (int pos = 0; pos < rowData.getArity(); pos++) {
            outputRowData.setField(pos, genericRowData.getField(pos).asString());
        }
        return outputRowData;
    }
}
