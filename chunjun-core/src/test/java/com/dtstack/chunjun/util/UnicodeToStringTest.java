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

package com.dtstack.chunjun.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author liuche
 * @date 2022/8/26 14:39
 */
public class UnicodeToStringTest {

    public static final Pattern pattern = Pattern.compile("(\\\\u(\\w{4}))");

    @Test
    public void testUnicodeToString() {
        String str = "UNISTR('\\5927\\6D77')";

        if (str.startsWith("UNISTR('") && str.endsWith("')")) {
            String substring = str.substring(8, str.length() - 2);
            String replace = substring.replace("\\", "\\u");
            str = unicodeToString(replace);
        }
        Assert.assertEquals(str, "大海");
    }

    public static String unicodeToString(String str) {
        Matcher matcher = pattern.matcher(str);
        char ch;
        while (matcher.find()) {
            ch = (char) Integer.parseInt(matcher.group(2), 16);
            str = str.replace(matcher.group(1), String.valueOf(ch));
        }
        return str;
    }
}
