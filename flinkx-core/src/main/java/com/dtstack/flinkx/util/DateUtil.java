/**
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

import org.apache.commons.lang3.time.FastDateFormat;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Date;
import java.util.TimeZone;

/**
 * Date Utilities
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class DateUtil {

    static final String timeZone = "GMT+8";

    static final String datetimeFormat = "yyyy-MM-dd HH:mm:ss";

    static final String dateFormat = "yyyy-MM-dd";

    static final String timeFormat = "HH:mm:ss";

    static final String yearFormat = "yyyy";

    static TimeZone timeZoner;

    static FastDateFormat datetimeFormatter;

    static FastDateFormat dateFormatter;

    static FastDateFormat timeFormatter;

    static FastDateFormat yearFormatter;

    private DateUtil() {}


    public static java.sql.Date columnToDate(Object column) {
        if(column == null) {
            return null;
        } else if(column instanceof String) {
            if (((String) column).length() == 0){
                return null;
            }
            return new java.sql.Date(stringToDate((String)column).getTime());
        } else if (column instanceof Integer) {
            Integer rawData = (Integer) column;
            return new java.sql.Date(rawData.longValue());
        } else if (column instanceof Long) {
            Long rawData = (Long) column;
            return new java.sql.Date(rawData.longValue());
        } else if (column instanceof java.sql.Date) {
            return (java.sql.Date) column;
        } else if(column instanceof Timestamp) {
            Timestamp ts = (Timestamp) column;
            return new java.sql.Date(ts.getTime());
        } else if(column instanceof Date) {
            Date d = (Date)column;
            return new java.sql.Date(d.getTime());
        }

        throw new IllegalArgumentException("Can't convert " + column.getClass().getName() + " to Date");
    }

    public static java.sql.Timestamp columnToTimestamp(Object column) {
        if (column == null) {
            return null;
        } else if(column instanceof String) {
            if (((String) column).length() == 0){
                return null;
            }
            return new java.sql.Timestamp(stringToDate((String)column).getTime());
        } else if (column instanceof Integer) {
            Integer rawData = (Integer) column;
            return new java.sql.Timestamp(rawData.longValue());
        } else if (column instanceof Long) {
            Long rawData = (Long) column;
            return new java.sql.Timestamp(rawData.longValue());
        } else if (column instanceof java.sql.Date) {
            return (java.sql.Timestamp) column;
        } else if(column instanceof Timestamp) {
            return (Timestamp) column;
        } else if(column instanceof Date) {
            Date d = (Date)column;
            return new java.sql.Timestamp(d.getTime());
        }

        throw new IllegalArgumentException("Can't convert " + column.getClass().getName() + " to Date");
    }

    public static Date stringToDate(String strDate)  {
        if(strDate == null || strDate.trim().length() == 0) {
            return null;
        }

        try {
            return datetimeFormatter.parse(strDate);
        } catch (ParseException ignored) {
        }

        try {
            return dateFormatter.parse(strDate);
        } catch (ParseException ignored) {
        }

        try {
            return timeFormatter.parse(strDate);
        } catch (ParseException ignored) {
        }

        try {
            return yearFormatter.parse(strDate);
        } catch (ParseException ignored) {
        }

        throw new RuntimeException("can't parse date");
    }

    public static String dateToString(Date date) {
        return dateFormatter.format(date);
    }

    public static String timestampToString(Date date) {
        return datetimeFormatter.format(date);
    }

    public static String dateToYearString(Date date) {
        return yearFormatter.format(date);
    }

    static {
        timeZoner = TimeZone.getTimeZone(timeZone);
        datetimeFormatter = FastDateFormat.getInstance(datetimeFormat, timeZoner);
        dateFormatter = FastDateFormat.getInstance(dateFormat, timeZoner);
        timeFormatter =  FastDateFormat.getInstance(timeFormat, timeZoner);
        yearFormatter = FastDateFormat.getInstance(yearFormat, timeZoner);
    }

}
