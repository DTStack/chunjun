/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.dtstack.chunjun.mapping;

import com.dtstack.chunjun.element.ColumnRowData;

import org.apache.flink.table.data.RowData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.dtstack.chunjun.constants.CDCConstantValue.DATABASE;
import static com.dtstack.chunjun.constants.CDCConstantValue.SCHEMA;
import static com.dtstack.chunjun.constants.CDCConstantValue.TABLE;

public interface Mapping<IN, OUT> {

    Set<String> META_HEADER =
            Stream.of("schema", "database", "table", "type", "opTime", "ts", "scn")
                    .collect(Collectors.toCollection(HashSet::new));

    /**
     * 匹配表名映射关系,根据映射关系修改RowData中表名等信息.
     *
     * @param value RowData
     * @return RowData
     */
    List<OUT> map(IN value);

    /**
     * 获取RowData中table、schema的index
     *
     * @param value RowData
     * @return index集
     */
    default Map<String, Integer> getIdentityIndex(RowData value) {
        Map<String, Integer> identityIndex = new HashMap<>(2);
        String[] headers = ((ColumnRowData) value).getHeaders();
        for (int i = 0; i < Objects.requireNonNull(headers).length; i++) {
            if (TABLE.equalsIgnoreCase(headers[i])) {
                identityIndex.put(TABLE, i);
            }

            if (SCHEMA.equalsIgnoreCase(headers[i])) {
                identityIndex.put(SCHEMA, i);
            }

            if (DATABASE.equalsIgnoreCase(headers[i])) {
                identityIndex.put(DATABASE, i);
            }
        }
        return identityIndex;
    }

    /**
     * 获取RowData中 field's index
     *
     * @param value ROwData
     * @return index集
     */
    default List<String> getFields(RowData value) {
        List<String> fields = new ArrayList<>(value.getArity());
        String[] headers = ((ColumnRowData) value).getHeaders();
        assert headers != null;
        for (String header : headers) {
            if (!META_HEADER.contains(header)) {
                fields.add(header);
            }
        }
        return fields;
    }
}
