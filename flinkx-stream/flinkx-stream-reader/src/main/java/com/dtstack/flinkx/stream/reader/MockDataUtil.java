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

package com.dtstack.flinkx.stream.reader;

import com.dtstack.flinkx.reader.MetaColumn;
import com.github.jsonzou.jmockdata.JMockData;
import com.github.jsonzou.jmockdata.MockConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flink.types.Row;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jiangbo
 * @date 2018/12/19
 */
public class MockDataUtil {

    private static AtomicLong id = new AtomicLong(0L);
    private static int minSize = 320;
    private static int maxSize = 320;
    private static MockConfig mockConfig = new MockConfig().subConfig(String.class).sizeRange(minSize,maxSize).globalConfig();

    public static Object mockData(String type){
        Object mockData;
        switch (type.trim().toLowerCase()){
            case "id": mockData = id.incrementAndGet();break;
            case "int":
            case "integer": mockData = JMockData.mock(int.class);break;
            case "byte": mockData = JMockData.mock(byte.class);break;
            case "boolean": mockData = JMockData.mock(boolean.class);break;
            case "char":
            case "character": mockData = JMockData.mock(char.class);break;
            case "short": mockData = JMockData.mock(short.class);break;
            case "long": mockData = JMockData.mock(long.class);break;
            case "float": mockData = JMockData.mock(float.class);break;
            case "double": mockData = JMockData.mock(double.class);break;
            case "date": mockData = JMockData.mock(Date.class);break;
            case "timestamp": mockData = JMockData.mock(Timestamp.class);break;
            case "bigdecimal": mockData = JMockData.mock(BigDecimal.class);break;
            case "biginteger": mockData = JMockData.mock(BigInteger.class);break;
            case "int[]": mockData = JMockData.mock(int[].class);break;
            case "byte[]": mockData = JMockData.mock(byte[].class);break;
            case "boolean[]": mockData = JMockData.mock(boolean[].class);break;
            case "char[]":
            case "character[]": mockData = JMockData.mock(char[].class);break;
            case "short[]": mockData = JMockData.mock(short[].class);break;
            case "long[]": mockData = JMockData.mock(long[].class);break;
            case "float[]": mockData = JMockData.mock(float[].class);break;
            case "double[]": mockData = JMockData.mock(double[].class);break;
            case "string[]": mockData = JMockData.mock(String[].class);break;
            case "binary":
                String str = JMockData.mock(String.class, mockConfig);
                mockData = new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8));break;
            default: mockData = JMockData.mock(String.class);break;
        }

        return mockData;
    }

    public static Row getMockRow(List<MetaColumn> columns){
        Row mockRow = new Row(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            if(columns.get(i).getValue() != null){
                if("null".equalsIgnoreCase(columns.get(i).getValue())){
                    mockRow.setField(i, null);
                } else {
                    mockRow.setField(i,getField(columns.get(i).getValue(), columns.get(i).getType()));
                }
            } else {
                mockRow.setField(i,mockData(columns.get(i).getType()));
            }
        }

        return mockRow;
    }

    private static Object getField(String value,String type){
        if (StringUtils.isEmpty(value)){
            return value;
        }

        Object field;
        switch (type.toLowerCase()){
            case "integer":
            case "smallint":
            case "tinyint":
            case "int" : field = NumberUtils.toInt(value);break;
            case "bigint" :
            case "long" : field = NumberUtils.toLong(value);break;
            case "float" : field = NumberUtils.toFloat(value);break;
            case "double" : field = NumberUtils.toDouble(value);break;
            default: field = value;
        }

        return field;
    }
}
