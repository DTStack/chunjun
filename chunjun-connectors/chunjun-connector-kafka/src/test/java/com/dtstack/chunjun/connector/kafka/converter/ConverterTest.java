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

package com.dtstack.chunjun.connector.kafka.converter;

import com.dtstack.chunjun.connector.kafka.conf.KafkaConf;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.util.GsonUtil;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;

public class ConverterTest {
    private final String kafkaConfContent =
            " {\n"
                    + "            \"topic\": \"da\",\n"
                    + "            \"groupId\": \"dddd\",\n"
                    + "            \"codec\": \"json\",\n"
                    + "    \"column\": [\n"
                    + "              {\n"
                    + "                \"name\": \"id\",\n"
                    + "                \"type\": \"int\"\n"
                    + "              },\n"
                    + "              {\n"
                    + "                \"name\": \"INTEGER\",\n"
                    + "                \"type\": \"INTEGER\"\n"
                    + "              },\n"
                    + "              {\n"
                    + "                \"name\": \"BOOLEAN\",\n"
                    + "                \"type\": \"BOOLEAN\"\n"
                    + "              },\n"
                    + "              {\n"
                    + "                \"name\": \"TINYINT\",\n"
                    + "                \"type\": \"TINYINT\"\n"
                    + "              },\n"
                    + "              {\n"
                    + "                \"name\": \"VARCHAR\",\n"
                    + "                \"type\": \"VARCHAR\"\n"
                    + "              },\n"
                    + "              {\n"
                    + "                \"name\": \"CHAR\",\n"
                    + "                \"type\": \"CHAR\"\n"
                    + "              },\n"
                    + "              {\n"
                    + "                \"name\": \"CHARACTER\",\n"
                    + "                \"type\": \"CHARACTER\"\n"
                    + "              },\n"
                    + "              {\n"
                    + "                \"name\": \"STRING\",\n"
                    + "                \"type\": \"STRING\"\n"
                    + "              },\n"
                    + "              {\n"
                    + "                \"name\": \"TEXT\",\n"
                    + "                \"type\": \"TEXT\"\n"
                    + "              },\n"
                    + "              {\n"
                    + "                \"name\": \"SHORT\",\n"
                    + "                \"type\": \"SHORT\"\n"
                    + "              },\n"
                    + "              {\n"
                    + "                \"name\": \"LONG\",\n"
                    + "                \"type\": \"LONG\"\n"
                    + "              },\n"
                    + "              {\n"
                    + "                \"name\": \"BIGINT\",\n"
                    + "                \"type\": \"BIGINT\"\n"
                    + "              },\n"
                    + "              {\n"
                    + "                \"name\": \"FLOAT\",\n"
                    + "                \"type\": \"FLOAT\"\n"
                    + "              },\n"
                    + "              {\n"
                    + "                \"name\": \"DOUBLE\",\n"
                    + "                \"type\": \"DOUBLE\"\n"
                    + "              },\n"
                    + "              {\n"
                    + "                \"name\": \"DECIMAL\",\n"
                    + "                \"type\": \"DECIMAL\"\n"
                    + "              },\n"
                    + "              {\n"
                    + "                \"name\": \"DATE\",\n"
                    + "                \"type\": \"DATE\"\n"
                    + "              },\n"
                    + "              {\n"
                    + "                \"name\": \"TIME\",\n"
                    + "                \"type\": \"TIME\"\n"
                    + "              },\n"
                    + "              {\n"
                    + "                \"name\": \"DATETIME\",\n"
                    + "                \"type\": \"DATETIME\"\n"
                    + "              },\n"
                    + "              {\n"
                    + "                \"name\": \"TIMESTAMP\",\n"
                    + "                \"type\": \"TIMESTAMP\"\n"
                    + "              }\n"
                    + "            ],"
                    + "            \"consumerSettings\": {\n"
                    + "              \"bootstrap.servers\": \"localhost:9092\",\n"
                    + "              \"auto.commit.enable\": \"false\"\n"
                    + "            }\n"
                    + "          }\n";

    @Test
    public void testConverter() throws Exception {
        KafkaConf kafkaConf = GsonUtil.GSON.fromJson(kafkaConfContent, KafkaConf.class);
        KafkaColumnConverter converter = new KafkaColumnConverter(kafkaConf);

        HashMap<String, Object> data = new HashMap<>();
        data.put("id", 1);
        data.put("INTEGER", 1);
        data.put("BOOLEAN", "true");
        data.put("TINYINT", "1");
        data.put("CHAR", "char");
        data.put("CHARACTER", "char");
        data.put("STRING", "tiezhu");
        data.put("VARCHAR", "dujie");
        data.put("TEXT", "abcdefg");
        data.put("SHORT", Short.parseShort("2"));
        data.put("LONG", 1234L);
        data.put("BIGINT", BigInteger.valueOf(12345L));
        data.put("FLOAT", Float.parseFloat("2.22"));
        data.put("DOUBLE", 2.22d);
        data.put("DECIMAL", new BigDecimal("2.22"));
        data.put("DATE", "2022-08-22");
        data.put("TIME", "18:00:00");
        data.put("DATETIME", "2022-08-12T18:00:00");
        data.put("TIMESTAMP", "2022-08-12 18:00:00");

        ColumnRowData rowData = (ColumnRowData) converter.toInternal(GsonUtil.GSON.toJson(data));
        Assert.assertEquals(19, rowData.getArity());

        Assert.assertEquals(1, (int) (rowData.getField(0).asInt()));
        Assert.assertEquals(1, (int) (rowData.getField(1).asInt()));
        Assert.assertTrue((rowData.getField(2).asBoolean()));
        Assert.assertEquals(1, (int) (rowData.getField(3).asInt()));
        Assert.assertEquals("dujie", (rowData.getField(4).asString()));
        Assert.assertEquals("char", (rowData.getField(5).asString()));
        Assert.assertEquals("char", (rowData.getField(6).asString()));
        Assert.assertEquals("tiezhu", (rowData.getField(7).asString()));
        Assert.assertEquals("abcdefg", (rowData.getField(8).asString()));
        Assert.assertEquals(2, (int) (rowData.getField(9).asInt()));
        Assert.assertEquals(1234L, (long) (rowData.getField(10).asLong()));
        Assert.assertEquals(12345L, (long) (rowData.getField(11).asLong()));
        Assert.assertEquals("2.22", (rowData.getField(12).asFloat().toString()));
        Assert.assertEquals("2.22", (rowData.getField(13).asDouble().toString()));
        Assert.assertEquals("2.22", (rowData.getField(14).asBigDecimal().toString()));
        Assert.assertEquals("2022-08-22", (rowData.getField(15).asString()));
        Assert.assertEquals(Time.valueOf("18:00:00"), (rowData.getField(16).asTime()));

        converter.toExternal(rowData, null);
        HashMap<String, Integer> headers = new HashMap<>(8);
        headers.put("schema", 0);
        rowData.setHeader(headers);
        converter.toExternal(rowData, null);
    }

    @Test
    public void testConverterWithTableFIelds() throws Exception {
        HashMap<String, Object> data = new HashMap<>();
        data.put("id", 1);
        data.put("INTEGER", 1);
        data.put("BOOLEAN", "true");
        data.put("TINYINT", "1");
        data.put("CHAR", "char");
        data.put("CHARACTER", "char");
        data.put("STRING", "tiezhu");
        data.put("VARCHAR", "dujie");
        data.put("TEXT", "abcdefg");
        data.put("SHORT", Short.parseShort("2"));
        data.put("LONG", 1234L);
        data.put("BIGINT", BigInteger.valueOf(12345L));
        data.put("FLOAT", Float.parseFloat("2.22"));
        data.put("DOUBLE", 2.22d);
        data.put("DECIMAL", new BigDecimal("2.22"));
        data.put("DATE", "2022-08-22");
        data.put("TIME", "18:00:00");
        data.put("DATETIME", "2022-08-12T18:00:00");
        data.put("TIMESTAMP", "2022-08-12 18:00:00");
        ArrayList<String> tableFields = new ArrayList<>();
        data.forEach(
                (k, v) -> {
                    tableFields.add(k);
                });
        KafkaConf kafkaConf = GsonUtil.GSON.fromJson(kafkaConfContent, KafkaConf.class);
        kafkaConf.setTableFields(tableFields);
        KafkaColumnConverter converter = new KafkaColumnConverter(kafkaConf);

        ColumnRowData rowData = (ColumnRowData) converter.toInternal(GsonUtil.GSON.toJson(data));
        Assert.assertEquals(19, rowData.getArity());

        Assert.assertEquals(1, (int) (rowData.getField(0).asInt()));
        Assert.assertEquals(1, (int) (rowData.getField(1).asInt()));
        Assert.assertTrue((rowData.getField(2).asBoolean()));
        Assert.assertEquals(1, (int) (rowData.getField(3).asInt()));
        Assert.assertEquals("dujie", (rowData.getField(4).asString()));
        Assert.assertEquals("char", (rowData.getField(5).asString()));
        Assert.assertEquals("char", (rowData.getField(6).asString()));
        Assert.assertEquals("tiezhu", (rowData.getField(7).asString()));
        Assert.assertEquals("abcdefg", (rowData.getField(8).asString()));
        Assert.assertEquals(2, (int) (rowData.getField(9).asInt()));
        Assert.assertEquals(1234L, (long) (rowData.getField(10).asLong()));
        Assert.assertEquals(12345L, (long) (rowData.getField(11).asLong()));
        Assert.assertEquals("2.22", (rowData.getField(12).asFloat().toString()));
        Assert.assertEquals("2.22", (rowData.getField(13).asDouble().toString()));
        Assert.assertEquals("2.22", (rowData.getField(14).asBigDecimal().toString()));
        Assert.assertEquals("2022-08-22", (rowData.getField(15).asString()));
        Assert.assertEquals(Time.valueOf("18:00:00"), (rowData.getField(16).asTime()));
        Assert.assertEquals(
                Timestamp.valueOf("2022-08-12 18:00:00"), (rowData.getField(17).asTimestamp()));
        Assert.assertEquals(
                Timestamp.valueOf("2022-08-12 18:00:00"), (rowData.getField(18).asTimestamp()));

        converter.toExternal(rowData, null);
        HashMap<String, Integer> headers = new HashMap<>(8);
        headers.put("schema", 0);
        rowData.setHeader(headers);
        converter.toExternal(rowData, null);
    }
}
