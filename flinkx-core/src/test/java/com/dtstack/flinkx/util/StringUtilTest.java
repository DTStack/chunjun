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

import org.apache.flink.types.Row;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author jiangbo
 * @date 2019/7/8
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({StringUtil.class, DateUtil.class})
public class StringUtilTest {

    public static String errorInfoPrefix = "Method " + StringUtil.class.getName();

    @Test
    public void convertRegularExprTest(){
        String result = StringUtil.convertRegularExpr(null);
        MatcherAssert.assertThat(errorInfoPrefix + ".convertRegularExpr must return empty string when param is null",
                result, Matchers.isEmptyString());

        result = StringUtil.convertRegularExpr("");
        MatcherAssert.assertThat(errorInfoPrefix + ".convertRegularExpr must return empty string when param is empty",
                result, Matchers.isEmptyString());

        List<String> specialChars = new ArrayList<>();
        specialChars.add("\\t");
        specialChars.add("\\r");
        specialChars.add("\\n");
        specialChars.add("\\122");

        StringBuilder testStr = new StringBuilder();
        for (String specialChar : specialChars) {
            testStr.append(" xxxx ").append(specialChar);
        }

        result = StringUtil.convertRegularExpr(testStr.toString());
        MatcherAssert.assertThat(result, Matchers.not(Matchers.containsString("\\")));
    }

    @Test
    public void row2stringTest() throws Exception{
        Row row = new Row(4);
        row.setField(0, "test");
        row.setField(1, "");
        row.setField(2, null);
        row.setField(3, "test");

        String result = StringUtil.row2string(row, Arrays.asList("STRING", "STRING", "STRING","STRING"), ",", null);
        MatcherAssert.assertThat("", result, Matchers.equalTo("test,,,test"));
    }
}
