/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.dtstack.flinkx.mapping;

import com.dtstack.flinkx.cdc.DdlRowData;
import com.dtstack.flinkx.element.ColumnRowData;

import org.apache.flink.table.data.RowData;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.dtstack.flinkx.constants.CDCConstantValue.SCHEMA;
import static com.dtstack.flinkx.constants.CDCConstantValue.TABLE;

/** @author shitou */
public interface Mapping {

    /**
     * 匹配表名映射关系,根据映射关系修改RowData中表名等信息.
     *
     * @param value RowData
     * @return RowData
     */
    RowData map(RowData value);

    /**
     * 获取RowData中table、schema的index
     *
     * @param value RowData
     * @return index集
     */
    default Map<String, Integer> getIdentityIndex(RowData value) {
        Map<String, Integer> identityIndex = new HashMap<>(2);
        // dml
        if (value instanceof ColumnRowData) {
            String[] headers = ((ColumnRowData) value).getHeaders();
            for (int i = 0; i < Objects.requireNonNull(headers).length; i++) {
                if (TABLE.equalsIgnoreCase(headers[i])) {
                    identityIndex.put(TABLE, i);
                }

                if (SCHEMA.equalsIgnoreCase(headers[i])) {
                    identityIndex.put(SCHEMA, i);
                }
            }
            return identityIndex;
        }

        // ddl
        if (value instanceof DdlRowData) {
            String[] headers = ((DdlRowData) value).getHeaders();
            for (int i = 0; i < Objects.requireNonNull(headers).length; i++) {
                if ("tableIdentifier".equalsIgnoreCase(headers[i])) {
                    identityIndex.put("tableIdentifier", i);
                }
            }
        }

        return identityIndex;
    }
}
