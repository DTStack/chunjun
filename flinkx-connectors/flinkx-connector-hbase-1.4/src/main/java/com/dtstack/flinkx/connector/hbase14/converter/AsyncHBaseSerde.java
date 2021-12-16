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

package com.dtstack.flinkx.connector.hbase14.converter;

import com.dtstack.flinkx.connector.hbase.HBaseSerde;
import com.dtstack.flinkx.connector.hbase.HBaseTableSchema;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.apache.hadoop.hbase.client.Result;

import java.util.Map;

/**
 * @program: flinkx
 * @author: wuren
 * @create: 2021/10/19
 */
public class AsyncHBaseSerde extends HBaseSerde {
    public AsyncHBaseSerde(HBaseTableSchema hbaseSchema, String nullStringLiteral) {
        super(hbaseSchema, nullStringLiteral);
    }

    /**
     * Converts HBase {@link Result} into a new {@link RowData} instance.
     *
     * <p>Note: this method is thread-safe.
     */
    public RowData convertToNewRow(Map<String, Map<String, byte[]>> result, byte[] rowkey) {
        // The output rows needs to be initialized each time
        // to prevent the possibility of putting the output object into the cache.
        GenericRowData resultRow = new GenericRowData(fieldLength);
        GenericRowData[] familyRows = new GenericRowData[families.length];
        for (int f = 0; f < families.length; f++) {
            familyRows[f] = new GenericRowData(qualifiers[f].length);
        }

        return convertToRow(result, resultRow, familyRows, rowkey);
    }

    protected RowData convertToRow(
            Map<String, Map<String, byte[]>> result,
            GenericRowData resultRow,
            GenericRowData[] familyRows,
            byte[] rowkey) {
        for (int i = 0; i < fieldLength; i++) {
            if (rowkeyIndex == i) {
                resultRow.setField(rowkeyIndex, keyDecoder.decode(rowkey));
            } else {
                int f = (rowkeyIndex != -1 && i > rowkeyIndex) ? i - 1 : i;
                // get family key
                byte[] familyKey = families[f];
                GenericRowData familyRow = familyRows[f];
                for (int q = 0; q < this.qualifiers[f].length; q++) {
                    // get quantifier key
                    byte[] qualifier = qualifiers[f][q];
                    // read value
                    if (result.get(new String(familyKey)) == null) {
                        familyRow.setField(q, null);
                        continue;
                    }
                    byte[] value = result.get(new String(familyKey)).get(new String(qualifier));
                    familyRow.setField(q, qualifierDecoders[f][q].decode(value));
                }
                resultRow.setField(i, familyRow);
            }
        }
        return resultRow;
    }
}
