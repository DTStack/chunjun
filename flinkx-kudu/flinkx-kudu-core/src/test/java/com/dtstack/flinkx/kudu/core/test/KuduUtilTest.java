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


package com.dtstack.flinkx.kudu.core.test;

import com.dtstack.flinkx.kudu.core.KuduUtil;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduPredicate;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

/**
 * @author jiangbo
 * @date 2019/8/12
 */
public class KuduUtilTest {

    @Test
    public void parseExpressTest(){
        Map<String, Type> nameTypeMap = new HashMap<>();
        nameTypeMap.put("id", Type.INT32);
        nameTypeMap.put("name", Type.STRING);
        nameTypeMap.put("time", Type.UNIXTIME_MICROS);

        KuduUtil.ExpressResult result = KuduUtil.parseExpress(" id >= 1", nameTypeMap);
        Assert.assertEquals(result.getColumnSchema().getName(), "id");
        Assert.assertEquals(result.getOp(), KuduPredicate.ComparisonOp.GREATER_EQUAL);
        Assert.assertTrue(result.getValue() instanceof Integer);

        result = KuduUtil.parseExpress("name = \"xxxxx\"", nameTypeMap);
        Assert.assertEquals(result.getColumnSchema().getName(), "name");
        Assert.assertEquals(result.getOp(), KuduPredicate.ComparisonOp.EQUAL);
        Assert.assertTrue(result.getValue() instanceof String);

        result = KuduUtil.parseExpress("time > 1565586665372 ", nameTypeMap);
        Assert.assertEquals(result.getColumnSchema().getName(), "time");
        Assert.assertEquals(result.getOp(), KuduPredicate.ComparisonOp.GREATER);
        Assert.assertTrue(result.getValue() instanceof Long);

        result = KuduUtil.parseExpress("time <= '2019-08-12 13:10:12'", nameTypeMap);
        Assert.assertEquals(result.getColumnSchema().getName(), "time");
        Assert.assertEquals(result.getOp(), KuduPredicate.ComparisonOp.LESS_EQUAL);
        Assert.assertTrue(result.getValue() instanceof Timestamp);
    }
}
