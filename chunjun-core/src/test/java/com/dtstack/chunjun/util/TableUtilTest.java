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

package com.dtstack.chunjun.util;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.converter.RawTypeConverter;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TableUtilTest {

    @Test
    public void testGetTypeInformation() {
        List<FieldConf> fieldConfList =
                ImmutableList.<FieldConf>builder()
                        .add(
                                FieldConf.getField(
                                        ImmutableMap.<String, Object>builder()
                                                .put("name", "id")
                                                .put("type", "int")
                                                .build(),
                                        1))
                        .add(
                                FieldConf.getField(
                                        ImmutableMap.<String, Object>builder()
                                                .put("name", "name")
                                                .put("type", "string")
                                                .put("customConverterClass", "Hello")
                                                .put("customConverterType", "Z")
                                                .build(),
                                        2))
                        .add(
                                FieldConf.getField(
                                        ImmutableMap.<String, Object>builder()
                                                .put("name", "comment")
                                                .put("type", "string")
                                                .put("value", "default")
                                                .build(),
                                        3))
                        .build();

        RawTypeConverter converter = new MockRawTypeConverter();
        TypeInformation<RowData> typeInformation =
                TableUtil.getTypeInformation(fieldConfList, converter, false);
        assertEquals(1, typeInformation.getTotalFields());
    }

    private class MockRawTypeConverter implements RawTypeConverter {

        @Override
        public DataType apply(String type) {
            switch (type) {
                case "string":
                    return DataTypes.STRING();
                case "int":
                    return DataTypes.INT();
                default:
                    return DataTypes.STRING();
            }
        }
    }
}
