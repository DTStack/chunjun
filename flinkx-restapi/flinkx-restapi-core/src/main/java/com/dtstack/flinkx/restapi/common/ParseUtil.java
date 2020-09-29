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
package com.dtstack.flinkx.restapi.common;

import org.apache.commons.lang.math.NumberUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * ParseUtil
 *
 * @author by dujie@dtstack.com
 * @Date 2020/9/27
 */
public class ParseUtil {

    public static List<Paramitem> parse(String data,ParamDefinition paramDefinition) {
        ArrayList<Paramitem> paramitems = new ArrayList<>(32);

        for (int i = 0, len = data.length(); i < len; i++) {
            //+-*/.()负数
            char tmp = data.charAt(i);
            if (tmp > '9' || tmp < '0') {//操作符
                if (tmp == '-' && (i == 0 || (data.charAt(i - 1) == '('))) {//负数的-号
                    int j = i;
                    i++;
                    while (i < len && '0' <= data.charAt(i) && data.charAt(i) <= '9') {
                        i++;
                    }
                    NumberUtils.isNumber(data.substring(j, i));
                    paramitems.add(new ConstantVarible(new BigDecimal(data.substring(j, i))));
                    i--;
                } else {
                    paramitems.add(new ConstantVarible(String.valueOf(tmp)));
                }
            } else {
                int j = i;
                while (i < len && '0' <= data.charAt(i) && data.charAt(i) <= '9' || data.charAt(i) == '.') {
                    i++;
                }
                NumberUtils.isNumber(data.substring(j, i));
                paramitems.add(new ConstantVarible(new BigDecimal(data.substring(j, i))));
                i--;
            }

        }
        return paramitems;
    }
}
