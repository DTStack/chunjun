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

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.Date;

public class StringUtilTest {

    @Test
    public void testConvertRegularExpr() {
        String result = StringUtil.convertRegularExpr("\\t\\r\\n");
        Assert.assertEquals(result, "\t\r\n");

        result = StringUtil.convertRegularExpr(null);
        Assert.assertEquals(result, "");
    }

    @Test
    public void testCol2string() {
        String result = StringUtil.col2string(null, null);
        Assert.assertEquals(result, "");

        result = StringUtil.col2string("test", null);
        Assert.assertEquals(result, "test");

        result = StringUtil.col2string(Byte.valueOf("1"), "TINYINT");
        Assert.assertEquals(result, "1");

        result = StringUtil.col2string(Short.valueOf("1"), "SMALLINT");
        Assert.assertEquals(result, "1");

        result = StringUtil.col2string(123, "INTEGER");
        Assert.assertEquals(result, "123");

        result = StringUtil.col2string(123L, "LONG");
        Assert.assertEquals(result, "123");

        result = StringUtil.col2string(new Timestamp(1584510286187L), "LONG");
        Assert.assertEquals(result, "1584510286187");

        result = StringUtil.col2string(123.123, "FLOAT");
        Assert.assertEquals(result, "123.123");

        result = StringUtil.col2string(123.123, "DOUBLE");
        Assert.assertEquals(result, "123.123");

        result = StringUtil.col2string(123.123, "DECIMAL");
        Assert.assertEquals(result, "123.123");

        result = StringUtil.col2string("string", "STRING");
        Assert.assertEquals(result, "string");

        result = StringUtil.col2string(new Timestamp(1584510286187L), "STRING");
        Assert.assertEquals(result, "2020-03-18 13:44:46");

        result = StringUtil.col2string(true, "BOOLEAN");
        Assert.assertEquals(result, "true");

        result = StringUtil.col2string(new Date(1584510286187L), "DATE");
        Assert.assertEquals(result, "2020-03-18");

        result = StringUtil.col2string(new Date(1584510286187L), "DATETIME");
        Assert.assertEquals(result, "2020-03-18 13:44:46");
    }

    @Test
    public void InputStream2String() throws IOException {
        String str = "text1\n newline\n newline2\t\n 中文";
        InputStream inputStream = new ByteArrayInputStream(str.getBytes());
        Assert.assertEquals(str, StringUtil.inputStream2String(inputStream));
    }
}
