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

import org.apache.commons.lang3.StringUtils;

import java.nio.CharBuffer;
import java.util.Locale;

public class PwdUtil {

    private static final String PASSWORD_KEY = "'password'";

    private static final String SINGLE_QUOTE = "'";

    private static final String SHADE_PWD = "******";

    private PwdUtil() throws IllegalAccessException {
        throw new IllegalAccessException(getClass() + " can not be instantiated.");
    }

    public static String desensitization(String content) {
        int length = content.length();
        String lowerCase = content.toLowerCase(Locale.ROOT);
        if (lowerCase.contains(PASSWORD_KEY)) {
            int index = lowerCase.indexOf(PASSWORD_KEY);
            CharBuffer charBuffer = CharBuffer.wrap(content);
            boolean start = false;
            int startIndex = 0;
            int endIndex = 0;
            for (int i = index + PASSWORD_KEY.length(); i < length; i++) {
                char c = charBuffer.charAt(i);
                String s = String.valueOf(c);
                if (StringUtils.isNotBlank(s)) {
                    if (start) {
                        if (SINGLE_QUOTE.equals(s)) {
                            endIndex = i;
                            break;
                        }
                    } else {
                        if (SINGLE_QUOTE.equals(s)) {
                            start = true;
                            startIndex = i;
                        }
                    }
                }
            }

            String pre = content.substring(0, startIndex + 1);
            String sub = content.substring(endIndex, length);

            return pre + SHADE_PWD + sub;
        }
        return content;
    }
}
