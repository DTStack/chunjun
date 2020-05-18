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

package com.dtstack.flinkx.hdfs.writer;

/**
 * String Utilities
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class StringUtil {

    private StringUtil() {}

    public static String join(String[] arr, String delimiter) {
        if (arr == null || arr.length == 0) {
            return null;
        }

        StringBuffer sb = new StringBuffer();
        for(int i = 0; i < arr.length; ++i) {
            if (i != 0) {
                sb.append(delimiter);
            }
            sb.append(arr[i]);
        }

        return sb.toString();
    }

    public static boolean isEmpty(String s) {
        return s == null || s.trim().length() == 0;
    }

    public static boolean isNotEmpty(String s) {
        return !isEmpty(s);
    }
}
