/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.carbondata.writer.dict;


import org.apache.hadoop.util.Shell;

import java.util.BitSet;

/**
 * external catalog utils
 *
 * Company: www.dtstack.com
 * @author huyifan_zju@163.com
 */
public class ExternalCatalogUtils {

    private static BitSet charToEscape = new BitSet(20);

    static {
        char[] clist = new char[] {
                '\u0001', '\u0002', '\u0003', '\u0004', '\u0005', '\u0006', '\u0007', '\u0008', '\u0009',
                '\n', '\u000B', '\u000C', '\r', '\u000E', '\u000F', '\u0010', '\u0011', '\u0012', '\u0013',
                '\u0014', '\u0015', '\u0016', '\u0017', '\u0018', '\u0019', '\u001A', '\u001B', '\u001C',
                '\u001D', '\u001E', '\u001F', '"', '#', '%', '\'', '*', '/', ':', '=', '?', '\\', '\u007F',
                '{', '[', ']', '^'
        };
        for(char c : clist) {
            charToEscape.set(c);
        }
        if (Shell.WINDOWS) {
            charToEscape.set(' ');
            charToEscape.set('<');
            charToEscape.set('>');
            charToEscape.set('|');
        }
    }

    public static String escapePathName(String path) {
        StringBuilder builder = new StringBuilder();
        for(int i = 0; i < path.length(); ++i) {
            char c = path.charAt(i);
            if(needsEscaping(c)) {
                builder.append('%');
                builder.append(String.format("%02X", (int)c));
            } else {
                builder.append(c);
            }
        }
        return builder.toString();
    }

    private static boolean needsEscaping(char c) {
        return c >= 0 && c < charToEscape.size() && charToEscape.get(c);
    }
}
