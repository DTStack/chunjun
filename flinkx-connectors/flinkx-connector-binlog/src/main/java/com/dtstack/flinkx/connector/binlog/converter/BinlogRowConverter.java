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
package com.dtstack.flinkx.connector.binlog.converter;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import com.dtstack.flinkx.connector.binlog.BinlogEventRow;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.IDeserializationConverter;
import com.dtstack.flinkx.converter.ISerializationConverter;

/**
 * Date: 2021/04/29
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class BinlogRowConverter extends AbstractRowConverter<BinlogEventRow, RowData, RowData, LogicalTypeRoot> {

    @Override
    protected ISerializationConverter wrapIntoNullableExternalConverter(ISerializationConverter serializationConverter, LogicalTypeRoot type) {
        return null;
    }

    @Override
    public RowData toInternal(BinlogEventRow binlogEventRow) throws Exception {
        return null;
    }

    @Override
    public RowData toInternalLookup(RowData input) throws Exception {
        return null;
    }

    @Override
    public RowData toExternal(RowData rowData, RowData output) throws Exception {
        return null;
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalTypeRoot type) {
        return null;
    }

    @Override
    protected ISerializationConverter createExternalConverter(LogicalTypeRoot type) {
        return null;
    }
}
