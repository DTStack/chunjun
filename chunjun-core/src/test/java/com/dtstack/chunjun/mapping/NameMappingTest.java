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

package com.dtstack.chunjun.mapping;

import com.dtstack.chunjun.constants.CDCConstantValue;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.StringColumn;

import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NameMappingTest {

    @Test
    public void testMap() {
        NameMappingConf nameMappingConf = new NameMappingConf();
        nameMappingConf.setSchemaMappings(ImmutableMap.of("schema_a", "schema_b"));
        nameMappingConf.setTableMappings(
                ImmutableMap.of("schema_a", ImmutableMap.of("table_a", "table_b")));
        nameMappingConf.setFieldMappings(
                ImmutableMap.of(
                        "schema_a",
                        ImmutableMap.of("table_a", ImmutableMap.of("field_a", "field_b"))));
        NameMapping nameMapping = new NameMapping(nameMappingConf);
        ColumnRowData columnRowData = new ColumnRowData(3);
        columnRowData.setHeader(
                new HashMap<>(
                        ImmutableMap.of(
                                CDCConstantValue.SCHEMA,
                                0,
                                CDCConstantValue.TABLE,
                                1,
                                "field_a",
                                2)));
        columnRowData.addField(new StringColumn("schema_a"));
        columnRowData.addField(new StringColumn("table_a"));
        nameMapping.map(columnRowData);
        String[] newHeaders = columnRowData.getHeaders();
        assert newHeaders != null;
        assertEquals("field_b", newHeaders[2]);

        assertEquals("schema_b", columnRowData.getField(0).asString());
        assertEquals("table_b", columnRowData.getField(1).asString());
    }
}
