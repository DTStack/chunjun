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

package com.dtstack.flinkx.hbase.writer;

import com.dtstack.flinkx.hbase.writer.function.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @company: www.dtstack.com
 * @author: toutian
 * @create: 2019/7/23
 */
public class RowKeyFunctionTest {

    @Test
    public void parseRowKeyColTest(){
        List<String> expectCol = new ArrayList<>();
        expectCol.add("col1");
        expectCol.add("col2");

        List<String> columnNames = FunctionParser.parseRowKeyCol("md5(test_$(col1)_test_$(col2)_test)");

        Assert.assertEquals(expectCol, columnNames);
    }

    @Test
    public void noFunc(){
        String express = "test_$(col1)_test_$(col2)_test";

        String expectVal = new StringFunction().evaluate("test_value1_test_value2_test");

        FunctionTree functionTree = FunctionParser.parse(express);

        Map<String, Object> nameValueMap = new HashMap<>();
        nameValueMap.put("col1", "value1");
        nameValueMap.put("col2", "value2");

        Assert.assertEquals(expectVal, functionTree.evaluate(nameValueMap));
    }

    @Test
    public void hasFunc(){
        String express = "md5(test_$(col1)_test_$(col2)_test)";

        String expectVal = new MD5Function().evaluate("test_value1_test_value2_test");

        FunctionTree functionTree = FunctionParser.parse(express);

        Map<String, Object> nameValueMap = new HashMap<>();
        nameValueMap.put("col1", "value1");
        nameValueMap.put("col2", "value2");

        Assert.assertEquals(expectVal, functionTree.evaluate(nameValueMap));
    }
}
