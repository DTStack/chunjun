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

import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

/**
 * @author jiangbo
 * @date 2020/3/18
 */
public class DateUtilTest {

    @Test
    public void testColumnToDate() {
        Date result = DateUtil.columnToDate(null, null);
        Assert.assertNull(result);

        result = DateUtil.columnToDate("", null);
        Assert.assertNull(result);

        result = DateUtil.columnToDate("2020-03-18 10:56:00", null);
        Assert.assertEquals(result, new Date(1584500160000L));

        result = DateUtil.columnToDate(1584500160, null);
        Assert.assertEquals(result, new Date(1584500160000L));

        result = DateUtil.columnToDate(1584500160000L, null);
        Assert.assertEquals(result, new Date(1584500160000L));

        result = DateUtil.columnToDate(new Date(1584500160000L), null);
        Assert.assertEquals(result, new Date(1584500160000L));

        result = DateUtil.columnToDate(new Timestamp(1584500160000L), null);
        Assert.assertEquals(result, new Date(1584500160000L));

        result = DateUtil.columnToDate(new java.util.Date(1584500160000L), null);
        Assert.assertEquals(result, new Date(1584500160000L));

        try {
            DateUtil.columnToDate(true, null);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testColumnToTimestamp() {
        Timestamp result = DateUtil.columnToTimestamp(null, null);
        Assert.assertNull(result);

        result = DateUtil.columnToTimestamp("", null);
        Assert.assertNull(result);

        result = DateUtil.columnToTimestamp("2020-03-18 10:56:00", null);
        Assert.assertEquals(result, new Timestamp(1584500160000L));

        result = DateUtil.columnToTimestamp(1584500160, null);
        Assert.assertEquals(result, new Timestamp(1584500160000L));

        result = DateUtil.columnToTimestamp(1584500160000L, null);
        Assert.assertEquals(result, new Timestamp(1584500160000L));

        result = DateUtil.columnToTimestamp(new Date(1584500160000L), null);
        Assert.assertEquals(result, new Timestamp(1584500160000L));

        result = DateUtil.columnToTimestamp(new Timestamp(1584500160000L), null);
        Assert.assertEquals(result, new Timestamp(1584500160000L));

        result = DateUtil.columnToTimestamp(new java.util.Date(1584500160000L), null);
        Assert.assertEquals(result, new Timestamp(1584500160000L));

        try {
            DateUtil.columnToTimestamp(true, null);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testGetMillSecond() {
        long result = DateUtil.getMillSecond("1584500160000");
        Assert.assertEquals(result, 1584500160000L);

        result = DateUtil.getMillSecond("1584500160000000");
        Assert.assertEquals(result, 1584500160000L);

        result = DateUtil.getMillSecond("1584500160000000000");
        Assert.assertEquals(result, 1584500160000L);

        long expect = 57600000; // 1970-01-02 00:00:00:000
        result = DateUtil.getMillSecond("1");
        Assert.assertEquals(result, expect);
    }

    @Test
    public void testStringToDate() {
        java.util.Date result = DateUtil.stringToDate("", null);
        Assert.assertNull(result);

        result =
                DateUtil.stringToDate(
                        "2020/03/18 13:26:00", new SimpleDateFormat("yyyy/MM/dd HH:mm:ss"));
        Assert.assertEquals(result, new java.util.Date(1584509160000L));

        try {
            DateUtil.stringToDate("xxxx", null);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("can't parse date"));
        }
    }
}
