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

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ColumnBuildUtilTest {

    @Test
    public void testHandleColumnList() {
        List<FieldConfig> fieldConfigList =
                ImmutableList.<FieldConfig>builder()
                        .add(
                                FieldConfig.getField(
                                        ImmutableMap.<String, Object>builder()
                                                .put("name", "id")
                                                .put("type", "int")
                                                .build(),
                                        1))
                        .add(
                                FieldConfig.getField(
                                        ImmutableMap.<String, Object>builder()
                                                .put("name", "name")
                                                .put("type", "string")
                                                .put("customConverterClass", "Hello")
                                                .put("customConverterType", "Z")
                                                .build(),
                                        2))
                        .add(
                                FieldConfig.getField(
                                        ImmutableMap.<String, Object>builder()
                                                .put("name", "comment")
                                                .put("type", "string")
                                                .put("value", "default")
                                                .build(),
                                        3))
                        .build();

        List<String> fullColumnList = ImmutableList.of("id", "name", "comment");
        List<TypeConfig> fullColumnTypeList =
                ImmutableList.of(
                        TypeConfig.fromString("int"),
                        TypeConfig.fromString("Z"),
                        TypeConfig.fromString("string"));
        Pair<List<String>, List<TypeConfig>> result =
                ColumnBuildUtil.handleColumnList(
                        fieldConfigList, fullColumnList, fullColumnTypeList);
        List<String> columnList = result.getLeft();
        List<TypeConfig> columnTypeList = result.getRight();
        assertTrue(columnList.contains("id"));
        assertTrue(columnList.contains("name"));
        assertFalse(columnList.contains("comment"));
        assertTrue(columnTypeList.contains("int"));
        assertTrue(columnTypeList.contains("Z"));
        assertFalse(columnTypeList.contains("string"));
    }

    @Test
    public void testHandleColumnListAndCanNotFindColumn() {
        List<FieldConfig> fieldConfigList =
                ImmutableList.<FieldConfig>builder()
                        .add(
                                FieldConfig.getField(
                                        ImmutableMap.<String, Object>builder()
                                                .put("name", "data")
                                                .put("type", "bytes")
                                                .build(),
                                        1))
                        .build();

        List<String> fullColumnList = ImmutableList.of("id", "name", "comment");
        List<TypeConfig> fullColumnTypeList =
                ImmutableList.of(
                        TypeConfig.fromString("int"),
                        TypeConfig.fromString("string"),
                        TypeConfig.fromString("string"));
        ChunJunRuntimeException thrown =
                assertThrows(
                        ChunJunRuntimeException.class,
                        () ->
                                ColumnBuildUtil.handleColumnList(
                                        fieldConfigList, fullColumnList, fullColumnTypeList),
                        "Expected handleColumnList() to throw, but it didn't");

        assertTrue(thrown.getMessage().contains("can not find field"));
    }

    @Test
    public void testHandleColumnListWithStar() {
        List<FieldConfig> fieldConfigList =
                ImmutableList.<FieldConfig>builder()
                        .add(
                                FieldConfig.getField(
                                        ImmutableMap.<String, Object>builder()
                                                .put("name", ConstantValue.STAR_SYMBOL)
                                                .build(),
                                        1))
                        .build();

        List<String> fullColumnList = ImmutableList.of("id", "name", "comment");
        List<TypeConfig> fullColumnTypeList =
                ImmutableList.of(
                        TypeConfig.fromString("int"),
                        TypeConfig.fromString("string"),
                        TypeConfig.fromString("string"));
        Pair<List<String>, List<TypeConfig>> result =
                ColumnBuildUtil.handleColumnList(
                        fieldConfigList, fullColumnList, fullColumnTypeList);
        List<String> columnList = result.getLeft();
        List<TypeConfig> columnTypeList = result.getRight();

        assertTrue(columnList.contains("id"));
        assertTrue(columnList.contains("name"));
        assertTrue(columnList.contains("comment"));
        assertTrue(columnTypeList.contains(TypeConfig.fromString("int")));
        assertTrue(columnTypeList.contains(TypeConfig.fromString("string")));
        assertTrue(columnTypeList.contains(TypeConfig.fromString("string")));
    }
}
