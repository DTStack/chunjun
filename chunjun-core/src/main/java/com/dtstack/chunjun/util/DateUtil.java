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

import cn.hutool.core.date.LocalDateTimeUtil;
import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.time.format.DateTimeFormatter.ISO_INSTANT;

public class DateUtil {

    private static final String TIME_ZONE = "GMT+8";

    private static final String STANDARD_DATETIME_FORMAT = "standardDatetimeFormatter";

    private static final String STANDARD_DATETIME_FORMAT_FOR_MILLISECOND =
            "standardDatetimeFormatterForMillisecond";

    private static final String UN_STANDARD_DATETIME_FORMAT = "unStandardDatetimeFormatter";

    private static final String DATE_FORMAT = "dateFormatter";

    private static final String TIME_FORMAT = "timeFormatter";

    private static final String YEAR_FORMAT = "yearFormatter";

    private static final String START_TIME = "1970-01-01";

    public static final String DATE_REGEX = "(?i)date";

    public static final String TIMESTAMP_REGEX = "(?i)timestamp";

    public static final String DATETIME_REGEX = "(?i)datetime";

    public static final int LENGTH_SECOND = 10;
    public static final int LENGTH_MILLISECOND = 13;
    public static final int LENGTH_MICROSECOND = 16;
    public static final int LENGTH_NANOSECOND = 19;

    static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    private static final Pattern DATETIME =
            Pattern.compile(
                    "^\\d{4}-(?:0[0-9]|1[0-2])-[0-9]{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{3,9})?Z$");
    private static final Pattern DATE = Pattern.compile("^\\d{4}-(?:0[0-9]|1[0-2])-[0-9]{2}$");
    private static final Pattern TIME = Pattern.compile("^\\d{2}:\\d{2}:\\d{2}(\\.\\d{3,9})?Z$");
    private static final Pattern TIMESTAMP_FORMAT_PATTERN =
            Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}?.*");
    private static final Pattern TIMESTAMP_FORMAT_WITH_TIMEZONE_PATTERN =
            Pattern.compile("([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})(.*)");
    private static final int MILLIS_PER_SECOND = 1000;

    /** parse yyyy-MM-dd HH:mm:ss.SSSSSS format string, like '2021-06-12 12:01:21.011101' * */
    public static final DateTimeFormatter DATETIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    public static ThreadLocal<Map<String, SimpleDateFormat>> datetimeFormatter =
            ThreadLocal.withInitial(
                    () -> {
                        TimeZone timeZone = TimeZone.getTimeZone(TIME_ZONE);

                        Map<String, SimpleDateFormat> formatterMap = new HashMap<>();
                        SimpleDateFormat standardDatetimeFormatter =
                                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        standardDatetimeFormatter.setTimeZone(timeZone);
                        formatterMap.put(STANDARD_DATETIME_FORMAT, standardDatetimeFormatter);

                        SimpleDateFormat unStandardDatetimeFormatter =
                                new SimpleDateFormat("yyyyMMddHHmmss");
                        unStandardDatetimeFormatter.setTimeZone(timeZone);
                        formatterMap.put(UN_STANDARD_DATETIME_FORMAT, unStandardDatetimeFormatter);

                        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
                        dateFormatter.setTimeZone(timeZone);
                        formatterMap.put(DATE_FORMAT, dateFormatter);

                        SimpleDateFormat timeFormatter = new SimpleDateFormat("HH:mm:ss");
                        timeFormatter.setTimeZone(timeZone);
                        formatterMap.put(TIME_FORMAT, timeFormatter);

                        SimpleDateFormat yearFormatter = new SimpleDateFormat("yyyy");
                        yearFormatter.setTimeZone(timeZone);
                        formatterMap.put(YEAR_FORMAT, yearFormatter);

                        SimpleDateFormat standardDatetimeFormatterOfMillisecond =
                                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        standardDatetimeFormatterOfMillisecond.setTimeZone(timeZone);
                        formatterMap.put(
                                STANDARD_DATETIME_FORMAT_FOR_MILLISECOND,
                                standardDatetimeFormatterOfMillisecond);

                        return formatterMap;
                    });

    private DateUtil() {}

    public static java.sql.Date columnToDate(Object column, SimpleDateFormat customTimeFormat) {
        if (column == null) {
            return null;
        } else if (column instanceof String) {
            if (((String) column).length() == 0) {
                return null;
            }

            Date date = stringToDate((String) column, customTimeFormat);
            if (null == date) {
                return null;
            }
            return new java.sql.Date(date.getTime());
        } else if (column instanceof Integer) {
            Integer rawData = (Integer) column;
            return new java.sql.Date(getMillSecond(rawData.toString()));
        } else if (column instanceof Long) {
            Long rawData = (Long) column;
            return new java.sql.Date(getMillSecond(rawData.toString()));
        } else if (column instanceof java.sql.Date) {
            return (java.sql.Date) column;
        } else if (column instanceof Timestamp) {
            Timestamp ts = (Timestamp) column;
            return new java.sql.Date(ts.getTime());
        } else if (column instanceof Date) {
            Date d = (Date) column;
            return new java.sql.Date(d.getTime());
        }

        throw new IllegalArgumentException(
                "Can't convert " + column.getClass().getName() + " to Date");
    }

    public static Timestamp columnToTimestamp(String data, String format) {
        LocalDateTime parse = LocalDateTimeUtil.parse(data, format);
        LocalTime localTime = parse.query(TemporalQueries.localTime());
        LocalDate localDate = parse.query(TemporalQueries.localDate());
        return Timestamp.valueOf(LocalDateTime.of(localDate, localTime));
    }

    public static java.sql.Timestamp columnToTimestamp(
            Object column, SimpleDateFormat customTimeFormat) {
        if (column == null) {
            return null;
        } else if (column instanceof String) {
            if (((String) column).length() == 0) {
                return null;
            }

            Date date = stringToDate((String) column, customTimeFormat);
            if (null == date) {
                return null;
            }
            return new java.sql.Timestamp(date.getTime());
        } else if (column instanceof Integer) {
            Integer rawData = (Integer) column;
            return new java.sql.Timestamp(getMillSecond(rawData.toString()));
        } else if (column instanceof Long) {
            Long rawData = (Long) column;
            return new java.sql.Timestamp(getMillSecond(rawData.toString()));
        } else if (column instanceof java.sql.Date) {
            return new java.sql.Timestamp(((java.sql.Date) column).getTime());
        } else if (column instanceof Timestamp) {
            return (Timestamp) column;
        } else if (column instanceof Date) {
            Date d = (Date) column;
            return new java.sql.Timestamp(d.getTime());
        }

        throw new UnsupportedOperationException(
                "Can't convert " + column.getClass().getName() + " to Date");
    }

    /** 将 2020-09-07 14:49:10.0 Timestamp */
    public static Timestamp convertToTimestamp(String timestamp) {
        if (TIMESTAMP_FORMAT_PATTERN.matcher(timestamp).find()) {
            return Timestamp.valueOf(timestamp);
        }
        return null;
    }

    public static long getMillSecond(String data) {
        long time = Long.parseLong(data);
        if (data.length() == LENGTH_SECOND) {
            time = Long.parseLong(data) * 1000;
        } else if (data.length() == LENGTH_MILLISECOND) {
            time = Long.parseLong(data);
        } else if (data.length() == LENGTH_MICROSECOND) {
            time = Long.parseLong(data) / 1000;
        } else if (data.length() == LENGTH_NANOSECOND) {
            time = Long.parseLong(data) / 1000000;
        } else if (data.length() < LENGTH_SECOND) {
            try {
                long day = Long.parseLong(data);
                Date date = datetimeFormatter.get().get(DATE_FORMAT).parse(START_TIME);
                Calendar cal = Calendar.getInstance();
                long addMill = date.getTime() + day * 24 * 3600 * 1000;
                cal.setTimeInMillis(addMill);
                time = cal.getTimeInMillis();
            } catch (Exception ignore) {
            }
        }
        return time;
    }

    public static Date stringToDate(String strDate, SimpleDateFormat customTimeFormat) {
        if (strDate == null || strDate.trim().length() == 0) {
            return null;
        }

        if (customTimeFormat != null) {
            try {
                return customTimeFormat.parse(strDate);
            } catch (ParseException ignored) {
            }
        }

        try {
            return datetimeFormatter.get().get(STANDARD_DATETIME_FORMAT).parse(strDate);
        } catch (ParseException ignored) {
        }

        try {
            return datetimeFormatter.get().get(UN_STANDARD_DATETIME_FORMAT).parse(strDate);
        } catch (ParseException ignored) {
        }

        try {
            return datetimeFormatter.get().get(DATE_FORMAT).parse(strDate);
        } catch (ParseException ignored) {
        }

        try {
            return datetimeFormatter.get().get(TIME_FORMAT).parse(strDate);
        } catch (ParseException ignored) {
        }

        try {
            return datetimeFormatter.get().get(YEAR_FORMAT).parse(strDate);
        } catch (ParseException ignored) {
        }

        throw new RuntimeException("can't parse date");
    }

    public static String dateToString(Date date) {
        return datetimeFormatter.get().get(DATE_FORMAT).format(date);
    }

    public static String timestampToString(Date date) {
        return datetimeFormatter.get().get(STANDARD_DATETIME_FORMAT).format(date);
    }

    public static String dateToYearString(Date date) {
        return datetimeFormatter.get().get(YEAR_FORMAT).format(date);
    }

    public static SimpleDateFormat getDateTimeFormatter() {
        return datetimeFormatter.get().get(STANDARD_DATETIME_FORMAT);
    }

    // 获取毫秒级别的日期解析
    public static SimpleDateFormat getDateTimeFormatterForMillisencond() {
        return datetimeFormatter.get().get(STANDARD_DATETIME_FORMAT_FOR_MILLISECOND);
    }

    public static SimpleDateFormat getDateFormatter() {
        return datetimeFormatter.get().get(DATE_FORMAT);
    }

    public static SimpleDateFormat getTimeFormatter() {
        return datetimeFormatter.get().get(TIME_FORMAT);
    }

    public static SimpleDateFormat getYearFormatter() {
        return datetimeFormatter.get().get(YEAR_FORMAT);
    }

    public static SimpleDateFormat buildDateFormatter(String timeFormat) {
        SimpleDateFormat sdf = new SimpleDateFormat(timeFormat);
        sdf.setTimeZone(TimeZone.getTimeZone(TIME_ZONE));
        return sdf;
    }

    /**
     * 常规自动日期格式识别
     *
     * @param str 时间字符串
     * @return String DateFormat字符串如：yyyy-MM-dd HH:mm:ss
     */
    public static String getDateFormat(String str) {
        if (StringUtils.isBlank(str)) {
            return null;
        }
        boolean year = false;
        Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
        if (pattern.matcher(str.substring(0, 4)).matches()) {
            year = true;
        }
        StringBuilder sb = new StringBuilder();
        int index = 0;
        if (!year) {
            if (str.contains("月") || str.contains("-") || str.contains("/")) {
                if (Character.isDigit(str.charAt(0))) {
                    index = 1;
                }
            } else {
                index = 3;
            }
        }
        for (int i = 0; i < str.length(); i++) {
            char chr = str.charAt(i);
            if (Character.isDigit(chr)) {
                if (index == 0) {
                    sb.append("y");
                }
                if (index == 1) {
                    sb.append("M");
                }
                if (index == 2) {
                    sb.append("d");
                }
                if (index == 3) {
                    sb.append("H");
                }
                if (index == 4) {
                    sb.append("m");
                }
                if (index == 5) {
                    sb.append("s");
                }
                if (index == 6) {
                    sb.append("S");
                }
            } else {
                if (i > 0) {
                    char lastChar = str.charAt(i - 1);
                    if (Character.isDigit(lastChar)) {
                        index++;
                    }
                }
                sb.append(chr);
            }
        }
        return sb.toString();
    }

    public static Timestamp getTimestampFromStr(String timeStr) {
        if (DATETIME.matcher(timeStr).matches()) {
            Instant instant = Instant.from(ISO_INSTANT.parse(timeStr));
            return new Timestamp(instant.getEpochSecond() * MILLIS_PER_SECOND);
        }

        TemporalAccessor parsedTimestamp = null;

        try {
            parsedTimestamp = DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(timeStr);
        } catch (Exception e) {
        }
        try {
            parsedTimestamp = DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(timeStr);
        } catch (Exception e) {
        }

        if (parsedTimestamp != null) {
            LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
            LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());
            return Timestamp.valueOf(LocalDateTime.of(localDate, localTime));
        }

        Date date = stringToDate(timeStr, null);
        return null == date ? null : new Timestamp(date.getTime());
    }

    public static Date stringToDate(String strDate) {
        if (strDate == null) {
            return null;
        }
        try {
            return localDateTimetoDate(LocalDateTime.parse(strDate, DATE_TIME_FORMATTER));
        } catch (DateTimeParseException ignored) {
        }

        try {
            return localDateTimetoDate(LocalDate.parse(strDate, DATE_FORMATTER).atStartOfDay());
        } catch (DateTimeParseException ignored) {
        }

        try {
            return localDateTimetoDate(
                    LocalDateTime.of(LocalDate.now(), LocalTime.parse(strDate, TIME_FORMATTER)));
        } catch (DateTimeParseException ignored) {
        }

        throw new RuntimeException("can't parse date");
    }

    public static Date localDateTimetoDate(LocalDateTime localDateTime) {
        return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
    }

    public static java.sql.Time getTimeFromStr(String dateStr) {
        if (TIME.matcher(dateStr).matches()) {
            dateStr = dateStr.substring(0, dateStr.length() - 1);
            Instant instant =
                    LocalTime.parse(dateStr).atDate(LocalDate.now()).toInstant(ZoneOffset.UTC);
            return new java.sql.Time(instant.toEpochMilli());
        } else if (DATETIME.matcher(dateStr).matches()) {
            Instant instant = Instant.from(ISO_INSTANT.parse(dateStr));
            return new java.sql.Time(instant.toEpochMilli());
        }
        Date date = stringToDate(dateStr);
        return null == date ? null : new java.sql.Time(date.getTime());
    }

    public static java.sql.Date getDateFromStr(String dateStr) {
        if (DATE.matcher(dateStr).matches()) {
            Instant instant =
                    LocalDate.parse(dateStr)
                            .atTime(LocalTime.of(0, 0, 0, 0))
                            .toInstant(ZoneOffset.UTC);
            int offset = TimeZone.getDefault().getOffset(instant.toEpochMilli());
            return new java.sql.Date(instant.toEpochMilli() - offset);
        } else if (DATETIME.matcher(dateStr).matches()) {
            Instant instant = Instant.from(ISO_INSTANT.parse(dateStr));
            return new java.sql.Date(instant.toEpochMilli());
        }
        Date date = stringToDate(dateStr);
        return null == date ? null : new java.sql.Date(date.getTime());
    }

    public static int getPrecisionFromTimestampStr(String timestampStr) {
        char radixPoint = '.';
        int length = timestampStr.length();
        for (int i = length - 1; i >= Math.max(0, length - 10); i--) {
            if (radixPoint == timestampStr.charAt(i)) {
                return length - i - 1;
            }
        }
        return -1;
    }

    public static Timestamp convertToTimestampWithZone(String timestamp) {
        Matcher matcher = TIMESTAMP_FORMAT_WITH_TIMEZONE_PATTERN.matcher(timestamp);
        if (matcher.find()) {
            return Timestamp.valueOf(matcher.group(1));
        }
        return null;
    }
}
