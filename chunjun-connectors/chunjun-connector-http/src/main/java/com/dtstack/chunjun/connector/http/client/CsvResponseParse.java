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

import com.csvreader.CsvReader;
import org.apache.commons.collections.CollectionUtils;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

public class CsvResponseParse extends ResponseParse {
    private CsvReader csvReader;
    private Reader reader;
    private String responseValue;
    private HttpRequestParam requestParam;

    public CsvResponseParse(HttpRestConfig config, AbstractRowConverter converter) {
        super(config, converter);
        if (CollectionUtils.isEmpty(columns)) {
            throw new RuntimeException("please configure column when decode is csv");
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        return csvReader.readRecord();
    }

    @Override
    public ResponseValue next() throws Exception {
        String[] data = csvReader.getValues();
        HashMap<String, Object> stringObjectHashMap = new HashMap<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getValue() != null) {
                stringObjectHashMap.put(columns.get(i).getName(), columns.get(i).getValue());
            } else {
                stringObjectHashMap.put(columns.get(i).getName(), data[i]);
            }
        }

        return new ResponseValue(
                converter.toInternal(stringObjectHashMap), requestParam, responseValue);
    }

    @Override
    public void parse(String responseValue, int responseStatus, HttpRequestParam requestParam) {
        this.responseValue = responseValue;
        this.requestParam = requestParam;
        this.reader = new StringReader(responseValue);
        this.csvReader = new CsvReader(reader);
        csvReader.setDelimiter(config.getCsvDelimiter().charAt(0));

        Map<String, Object> csvConfig = config.getCsvConfig();
        // 是否跳过空行
        csvReader.setSkipEmptyRecords((Boolean) csvConfig.getOrDefault("skipEmptyRecords", true));
        // 是否使用csv转义字符
        csvReader.setUseTextQualifier((Boolean) csvConfig.getOrDefault("useTextQualifier", true));
        csvReader.setTrimWhitespace((Boolean) csvConfig.getOrDefault("trimWhitespace", false));
        // 单列长度是否限制100000字符
        csvReader.setSafetySwitch((Boolean) csvConfig.getOrDefault("safetySwitch", false));
    }
}
