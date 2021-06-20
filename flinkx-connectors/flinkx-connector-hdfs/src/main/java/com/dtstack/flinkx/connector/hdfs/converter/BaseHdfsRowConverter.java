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
package com.dtstack.flinkx.connector.hdfs.converter;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.throwable.FlinkxRuntimeException;

/**
 * Date: 2021/06/16
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public abstract class BaseHdfsRowConverter<T> extends AbstractRowConverter<RowData, RowData, RowData, T> {

    public BaseHdfsRowConverter(RowType rowType) {
        super(rowType);
    }

    public BaseHdfsRowConverter(){}

    public BaseHdfsRowConverter(int converterSize) {
        super(converterSize);
    }

    @Override
    @SuppressWarnings("unchecked")
    public RowData toInternal(RowData input) {
        GenericRowData row = new GenericRowData(input.getArity());
        if(input instanceof GenericRowData){
            GenericRowData genericRowData = (GenericRowData) input;
            for (int i = 0; i < input.getArity(); i++) {
                row.setField(i, toInternalConverters[i].deserialize(genericRowData.getField(i)));
            }
        }else{
            throw new FlinkxRuntimeException("Error RowData type, RowData:[" + input + "] should be instance of GenericRowData.");
        }
        return row;
    }

    @Override
    @SuppressWarnings("unchecked")
    public RowData toExternal(RowData rowData, RowData output) throws Exception {
        for (int index = 0; index < rowData.getArity(); index++) {
            toExternalConverters[index].serialize(rowData, index, output);
        }
        return output;
    }

    @Override
    public RowData toInternalLookup(RowData input) {
        throw new FlinkxRuntimeException("HDFS Connector doesn't support Lookup Table Function.");
    }
}
