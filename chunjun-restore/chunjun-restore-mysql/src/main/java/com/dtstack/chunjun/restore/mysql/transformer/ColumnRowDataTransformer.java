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

package com.dtstack.chunjun.restore.mysql.transformer;

import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.restore.mysql.util.TransformerUtil;

import org.apache.flink.types.RowKind;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.LinkedHashMap;

public class ColumnRowDataTransformer {

    private ColumnRowDataTransformer() throws IllegalAccessException {
        throw new IllegalAccessException(getClass() + " can not be instantiated.");
    }

    public static ColumnRowData transform(String in) {
        JSONObject jsonObject = JSONObject.parseObject(in);

        JSONArray columnList = jsonObject.getJSONArray("columnList");
        int byteSize = jsonObject.getIntValue("byteSize");
        String rowKind = jsonObject.getString("kind");
        JSONObject header = jsonObject.getJSONObject("header");
        JSONArray extHeader = jsonObject.getJSONArray("extHeader");

        LinkedHashMap<String, Integer> headerMap = new LinkedHashMap<>();

        ColumnRowData columnRowData =
                new ColumnRowData(RowKind.valueOf(rowKind), header.size(), byteSize);

        header.keySet().forEach(item -> headerMap.put(item, (int) header.get(item)));
        columnList.forEach(
                item -> {
                    JSONObject object = (JSONObject) item;
                    AbstractBaseColumn column = ColumnTransformer.transform(object);
                    columnRowData.addFieldWithOutByteSize(column);
                });

        columnRowData.setHeader(TransformerUtil.sortHeaders(headerMap));
        extHeader.forEach(item -> columnRowData.addExtHeader(String.valueOf(item)));

        return columnRowData;
    }
}
