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

package com.dtstack.chunjun.connector.hbase14.converter;

import com.dtstack.chunjun.connector.hbase.HBaseTableSchema;
import com.dtstack.chunjun.converter.AbstractRowConverter;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;

public class HbaseRowConverter
        extends AbstractRowConverter<Result, RowData, Mutation, LogicalType> {
    private HBaseTableSchema schema;
    private String nullStringLiteral;
    private transient HBaseSerde serde;

    public HbaseRowConverter(HBaseTableSchema schema, String nullStringLiteral) {
        //        super(rowType);
        this.schema = schema;
        this.nullStringLiteral = nullStringLiteral;
    }

    @Override
    public RowData toInternal(Result input) throws Exception {
        if (serde == null) {
            this.serde = new HBaseSerde(schema, nullStringLiteral);
        }

        return serde.convertToRow(input);
    }

    @Override
    public Mutation toExternal(RowData rowData, Mutation output) throws Exception {
        if (serde == null) {
            this.serde = new HBaseSerde(schema, nullStringLiteral);
        }
        RowKind kind = rowData.getRowKind();
        if (kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER) {
            return serde.createPutMutation(rowData);
        } else {
            return serde.createDeleteMutation(rowData);
        }
    }

    @Override
    public RowData toInternalLookup(RowData input) throws Exception {
        return input;
    }
}
