/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.util;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author jiangbo
 * @date 2020/3/12
 */
public class StringUtilTest {

    @Test
    public void testConvertRegularExpr() {
        String result = StringUtil.convertRegularExpr("\\t\\r\\n");
        Assert.assertEquals(result, "\t\r\n");

        result = StringUtil.convertRegularExpr(null);
        Assert.assertEquals(result, "");
    }

    @Test
    public void testString2col() {
        Object result = StringUtil.string2col("", "string", null);
        Assert.assertEquals(result, "");

        result = StringUtil.string2col("123", "TINYINT", null);
        Assert.assertEquals(result, Byte.valueOf("123"));

        result = StringUtil.string2col("1", "SMALLINT", null);
        Assert.assertEquals(result, (short)1);

        result = StringUtil.string2col("1", "INT", null);
        Assert.assertEquals(result, 1);

        result = StringUtil.string2col("1", "MEDIUMINT", null);
        Assert.assertEquals(result, (long)1);

        result = StringUtil.string2col("1", "BIGINT", null);
        Assert.assertEquals(result, (long)1);

        result = StringUtil.string2col("1.1", "float", null);
        Assert.assertEquals(result, (float)1.1);

        result = StringUtil.string2col("1.1", "double", null);
        Assert.assertEquals(result, 1.1);

        result = StringUtil.string2col("value", "string", null);
        Assert.assertEquals(result, "value");

        result = StringUtil.string2col("value", "string", null);
        Assert.assertEquals(result, "value");

        result = StringUtil.string2col("20200312-174412", "string", new SimpleDateFormat("yyyyMMdd-HHmmss"));
        Assert.assertEquals(result, "2020-03-12 17:44:12");

        result = StringUtil.string2col("true", "boolean", null);
        Assert.assertEquals(result, true);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd-HHmmss");
        java.sql.Date date = new java.sql.Date(System.currentTimeMillis());
        result = StringUtil.string2col(sdf.format(date), "date", sdf);
        Assert.assertEquals(result.toString(), date.toString());

        result = StringUtil.string2col("xxx", "xxx", null);
        Assert.assertEquals(result, "xxx");
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
}