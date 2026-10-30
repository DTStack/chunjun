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
package com.dtstack.chunjun.connector.binlog.converter;

import com.dtstack.chunjun.connector.binlog.listener.BinlogEventRow;
import com.dtstack.chunjun.element.ColumnRowData;

import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import com.alibaba.otter.canal.protocol.CanalEntry;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.Map;

public class BinlogValueConversionTest {
    @Test
    public void preservesNumericAndLegacyBooleanValues() throws Exception {
        for (String value : new String[] {"1", "0", "true", "false", "TRUE"}) {
            for (boolean paving : new boolean[] {false, true}) {
                for (boolean split : new boolean[] {false, true}) {
                    if (!paving && !split) {
                        continue;
                    }
                    ColumnRowData row =
                            convert(CanalEntry.EventType.INSERT, paving, split, value, true)
                                    .getFirst();
                    String field = split ? "enabled" : "after_enabled";
                    Assert.assertEquals(
                            value,
                            "1".equals(value) || Boolean.parseBoolean(value),
                            row.getField(field).asBoolean());
                }
            }
        }
    }

    @Test
    public void nestedMapsPreserveNullAndExistingNonNullTypes() throws Exception {
        for (CanalEntry.EventType event : events()) {
            ColumnRowData row = convert(event, false, false, "1", true).getFirst();
            if (event != CanalEntry.EventType.INSERT) {
                assertMap(row, "before", true);
            }
            if (event != CanalEntry.EventType.DELETE) {
                assertMap(row, "after", true);
            }
        }
    }

    @Test
    public void nonNullMapDecimalsRemainStrings() throws Exception {
        ColumnRowData row =
                convert(CanalEntry.EventType.UPDATE, false, false, "1", false).getFirst();
        assertMap(row, "before", false);
        assertMap(row, "after", false);
    }

    @Test
    public void preservesRowKindsAndNullsAcrossOutputModes() throws Exception {
        for (CanalEntry.EventType event : events()) {
            for (boolean paving : new boolean[] {false, true}) {
                for (boolean split : new boolean[] {false, true}) {
                    LinkedList<ColumnRowData> rows = convert(event, paving, split, "0", true);
                    Assert.assertEquals(
                            split && event == CanalEntry.EventType.UPDATE ? 2 : 1, rows.size());
                    for (int i = 0; i < rows.size(); i++) {
                        ColumnRowData row = rows.get(i);
                        RowKind expected =
                                event == CanalEntry.EventType.DELETE
                                        ? RowKind.DELETE
                                        : RowKind.INSERT;
                        if (split && event == CanalEntry.EventType.UPDATE) {
                            expected = i == 0 ? RowKind.UPDATE_BEFORE : RowKind.UPDATE_AFTER;
                        }
                        Assert.assertEquals(expected, row.getRowKind());
                        if (split) {
                            Assert.assertNull(row.getField("amount").getData());
                        } else if (paving) {
                            if (event != CanalEntry.EventType.INSERT) {
                                Assert.assertNull(row.getField("before_amount").getData());
                            }
                            if (event != CanalEntry.EventType.DELETE) {
                                Assert.assertNull(row.getField("after_amount").getData());
                            }
                        }
                        Assert.assertEquals("sample_schema", row.getField("schema").asString());
                        Assert.assertEquals("sample_table", row.getField("table").asString());
                    }
                }
            }
        }
    }

    private static void assertMap(ColumnRowData row, String header, boolean nullAmount) {
        Map<?, ?> values = (Map<?, ?>) row.getField(header).getData();
        Assert.assertTrue(values.containsKey("amount"));
        Assert.assertEquals(nullAmount ? null : "12.50", values.get("amount"));
        Assert.assertEquals("", values.get("label"));
        Assert.assertEquals("1", values.get("enabled"));
    }

    private static CanalEntry.EventType[] events() {
        return new CanalEntry.EventType[] {
            CanalEntry.EventType.INSERT, CanalEntry.EventType.UPDATE, CanalEntry.EventType.DELETE
        };
    }

    private static LinkedList<ColumnRowData> convert(
            CanalEntry.EventType event,
            boolean paving,
            boolean split,
            String bit,
            boolean nullAmount)
            throws Exception {
        CanalEntry.RowData.Builder row = CanalEntry.RowData.newBuilder();
        CanalEntry.Column[] columns = {
            column("enabled", "bit(1)", bit, false),
            column("amount", "decimal(10,2)", nullAmount ? "" : "12.50", nullAmount),
            column("label", "varchar(20)", "", false)
        };
        for (CanalEntry.Column column : columns) {
            if (event != CanalEntry.EventType.INSERT) {
                row.addBeforeColumns(column);
            }
            if (event != CanalEntry.EventType.DELETE) {
                row.addAfterColumns(column);
            }
        }
        CanalEntry.RowChange change =
                CanalEntry.RowChange.newBuilder().setEventType(event).addRowDatas(row).build();
        BinlogEventRow input =
                new BinlogEventRow(
                        change, "sample_schema", "sample_table", 1000L, "sample-bin.000001/100");
        LinkedList<ColumnRowData> result = new LinkedList<>();
        for (RowData output : new BinlogSyncConverter(paving, split).toInternal(input)) {
            result.add((ColumnRowData) output);
        }
        return result;
    }

    private static CanalEntry.Column column(
            String name, String type, String value, boolean isNull) {
        return CanalEntry.Column.newBuilder()
                .setName(name)
                .setMysqlType(type)
                .setValue(value)
                .setIsNull(isNull)
                .build();
    }
}
