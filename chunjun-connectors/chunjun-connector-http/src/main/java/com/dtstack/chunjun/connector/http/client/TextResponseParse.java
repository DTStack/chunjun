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

package com.dtstack.chunjun.connector.http.client;

import com.dtstack.chunjun.connector.http.common.HttpRestConfig;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.StringColumn;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.Iterator;

public class TextResponseParse extends ResponseParse {
    private Iterator<String> iterator;
    private HttpRequestParam requestParam;
    private String responseValue;

    public TextResponseParse(HttpRestConfig config, AbstractRowConverter converter) {
        super(config, converter);
    }

    @Override
    public boolean hasNext() throws IOException {
        return iterator.hasNext();
    }

    @Override
    public ResponseValue next() throws Exception {
        ColumnRowData columnRowData = new ColumnRowData(1);
        columnRowData.addField(new StringColumn(iterator.next()));
        return new ResponseValue(columnRowData, requestParam, responseValue);
    }

    @Override
    public void parse(String responseValue, int responseStatus, HttpRequestParam requestParam) {
        this.responseValue = responseValue;
        this.requestParam = requestParam;
        this.iterator = Lists.newArrayList(responseValue).iterator();
    }
}
