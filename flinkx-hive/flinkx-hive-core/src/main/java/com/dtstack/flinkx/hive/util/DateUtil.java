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

package com.dtstack.flinkx.hive.util;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

/**
 * @author toutian
 */
public class DateUtil {

    private static final String TIME_ZONE = "GMT+8";

    private static final String STANDARD_DATETIME_FORMAT = "standardDatetimeFormatter";

    private static final String UN_STANDARD_DATETIME_FORMAT = "unStandardDatetimeFormatter";

    private static final String DATE_FORMAT = "dateFormatter";

    private static final String TIME_FORMAT = "timeFormatter";

    private static final String YEAR_FORMAT = "yearFormatter";

    private static final String DAY_FORMAT = "dayFormatter";

    private static final String HOUR_FORMAT = "hourFormatter";

    private static final String MINUTE_FORMAT = "minuteFormatter";

    public static ThreadLocal<Map<String,SimpleDateFormat>> datetimeFormatter = ThreadLocal.withInitial(() -> {
        TimeZone timeZone = TimeZone.getTimeZone(TIME_ZONE);

        Map<String, SimpleDateFormat> formatterMap = new HashMap<>();

        SimpleDateFormat standardDatetimeFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        standardDatetimeFormatter.setTimeZone(timeZone);
        formatterMap.put(STANDARD_DATETIME_FORMAT,standardDatetimeFormatter);

        SimpleDateFormat unStandardDatetimeFormatter = new SimpleDateFormat("yyyyMMddHHmmss");
        unStandardDatetimeFormatter.setTimeZone(timeZone);
        formatterMap.put(UN_STANDARD_DATETIME_FORMAT,unStandardDatetimeFormatter);

        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
        dateFormatter.setTimeZone(timeZone);
        formatterMap.put(DATE_FORMAT,dateFormatter);

        SimpleDateFormat timeFormatter = new SimpleDateFormat("HH:mm:ss");
        timeFormatter.setTimeZone(timeZone);
        formatterMap.put(TIME_FORMAT,timeFormatter);

        SimpleDateFormat yearFormatter = new SimpleDateFormat("yyyy");
        yearFormatter.setTimeZone(timeZone);
        formatterMap.put(YEAR_FORMAT,yearFormatter);

        SimpleDateFormat dayFormatter = new SimpleDateFormat("yyyyMMdd");
        dayFormatter.setTimeZone(timeZone);
        formatterMap.put(DAY_FORMAT,dayFormatter);

        SimpleDateFormat hourFormatter = new SimpleDateFormat("yyyyMMddHH");
        hourFormatter.setTimeZone(timeZone);
        formatterMap.put(HOUR_FORMAT,hourFormatter);

        SimpleDateFormat minuteFormatter = new SimpleDateFormat("yyyyMMddHHmm");
        minuteFormatter.setTimeZone(timeZone);
        formatterMap.put(MINUTE_FORMAT,minuteFormatter);

        return formatterMap;
    });

    private DateUtil() {}

    public static SimpleDateFormat getDayFormatter(){
        return datetimeFormatter.get().get(DAY_FORMAT);
    }

    public static SimpleDateFormat getHourFormatter(){
        return datetimeFormatter.get().get(HOUR_FORMAT);
    }

    public static SimpleDateFormat getMinuteFormatter(){
        return datetimeFormatter.get().get(MINUTE_FORMAT);
    }
}
