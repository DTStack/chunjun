/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.kafkabase.util;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Date: 2019/11/21
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class Formatter {
    private static Pattern p = Pattern.compile("(%\\{.*?})");
    private static DateTimeFormatter ISOformatter = ISODateTimeFormat.dateTimeParser().withOffsetParsed();

    public static String format(Map event, String format) {
        return format(event, format, "UTC");
    }

    public static String format(Map event, String format, String timezone) {
        Matcher m = p.matcher(format);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String match = m.group();
            String key = (String) match.subSequence(2, match.length() - 1);
            if ("+s".equalsIgnoreCase(key)) {
                Object o = event.get("@timestamp");
                if (o.getClass() == Long.class) {
                    m.appendReplacement(sb, o.toString());
                }
            } else if (key.startsWith("+")) {
                DateTimeFormatter formatter = DateTimeFormat.forPattern(
                        (String) key.subSequence(1, key.length())).withZone(
                        DateTimeZone.forID(timezone));
                Object o = event.get("@timestamp");
                if (o == null) {
                    DateTime timestamp = new DateTime();
                    m.appendReplacement(sb, timestamp.toString(formatter));
                } else {
                    if (o.getClass() == DateTime.class) {
                        m.appendReplacement(sb,
                                ((DateTime) o).toString(formatter));
                    } else if (o.getClass() == Long.class) {
                        DateTime timestamp = new DateTime((Long) o);
                        m.appendReplacement(sb, timestamp.toString(formatter));
                    } else if (o.getClass() == String.class) {
                        DateTime timestamp = ISOformatter
                                .parseDateTime((String) o);
                        m.appendReplacement(sb, timestamp.toString(formatter));
                    }
                }
            } else if (event.containsKey(key)) {
                m.appendReplacement(sb, event.get(key).toString());
            }

        }
        m.appendTail(sb);
        return sb.toString();
    }

    public static boolean isFormat(String format) {
        Matcher m = p.matcher(format);
        return m.find();
    }
}