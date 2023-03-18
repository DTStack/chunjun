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
package com.dtstack.chunjun.element.column;

import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.throwable.CastException;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

public class ZonedTimestampColumn extends AbstractBaseColumn {

    private static final int defaultMillisecondOffset = TimeZone.getDefault().getRawOffset();
    private final int nanos;
    /** timeZone offset */
    private final int millisecondOffset;

    private final int precision;

    public ZonedTimestampColumn(Timestamp data) {
        super(data.getTime(), 16);
        this.nanos = data.getNanos();
        this.millisecondOffset = defaultMillisecondOffset;
        this.precision = 6;
    }

    public ZonedTimestampColumn(Timestamp data, int precision) {
        super(data.getTime(), 16);
        this.nanos = data.getNanos();
        this.millisecondOffset = defaultMillisecondOffset;
        this.precision = precision;
    }

    public ZonedTimestampColumn(Timestamp data, TimeZone timeZone) {
        super(data.getTime(), 16);
        this.nanos = data.getNanos();
        this.millisecondOffset = timeZone.getRawOffset();
        this.precision = 6;
    }

    public ZonedTimestampColumn(Timestamp data, TimeZone timeZone, int precision) {
        super(data.getTime(), 16);
        this.nanos = data.getNanos();
        this.millisecondOffset = timeZone.getRawOffset();
        this.precision = precision;
    }

    public ZonedTimestampColumn(Timestamp data, ZoneId zoneId) {
        super(data.getTime(), 16);
        this.nanos = data.getNanos();
        this.millisecondOffset = TimeZone.getTimeZone(zoneId).getRawOffset();
        this.precision = 6;
    }

    public ZonedTimestampColumn(Timestamp data, ZoneId zoneId, int precision) {
        super(data.getTime(), 16);
        this.nanos = data.getNanos();
        this.millisecondOffset = TimeZone.getTimeZone(zoneId).getRawOffset();
        this.precision = precision;
    }

    public ZonedTimestampColumn(Date data) {
        super(data.getTime(), 16);
        this.nanos = (int) (data.getTime() % 1000 * 1000000);
        this.millisecondOffset = defaultMillisecondOffset;
        this.precision = 6;
    }

    public ZonedTimestampColumn(Date data, int precision) {
        super(data.getTime(), 16);
        this.nanos = (int) (data.getTime() % 1000 * 1000000);
        this.millisecondOffset = defaultMillisecondOffset;
        this.precision = precision;
    }

    public ZonedTimestampColumn(Date data, TimeZone timeZone) {
        super(data.getTime(), 16);
        this.nanos = (int) (data.getTime() % 1000 * 1000000);
        this.millisecondOffset = timeZone.getRawOffset();
        this.precision = 6;
    }

    public ZonedTimestampColumn(Date data, TimeZone timeZone, int precision) {
        super(data.getTime(), 16);
        this.nanos = (int) (data.getTime() % 1000 * 1000000);
        this.millisecondOffset = timeZone.getRawOffset();
        this.precision = precision;
    }

    public ZonedTimestampColumn(Date data, ZoneId zoneId) {
        super(data.getTime(), 16);
        this.nanos = (int) (data.getTime() % 1000 * 1000000);
        this.millisecondOffset = TimeZone.getTimeZone(zoneId).getRawOffset();
        this.precision = 6;
    }

    public ZonedTimestampColumn(Date data, ZoneId zoneId, int precision) {
        super(data.getTime(), 16);
        this.nanos = (int) (data.getTime() % 1000 * 1000000);
        this.millisecondOffset = TimeZone.getTimeZone(zoneId).getRawOffset();
        this.precision = precision;
    }

    public ZonedTimestampColumn(long data) {
        super(data, 16);
        this.nanos = (int) (data % 1000 * 1000000);
        this.millisecondOffset = defaultMillisecondOffset;
        this.precision = 6;
    }

    public ZonedTimestampColumn(long data, int precision) {
        super(data, 16);
        this.nanos = (int) (data % 1000 * 1000000);
        this.millisecondOffset = defaultMillisecondOffset;
        this.precision = precision;
    }

    public ZonedTimestampColumn(long data, TimeZone timeZone) {
        super(data, 16);
        this.nanos = (int) (data % 1000 * 1000000);
        this.millisecondOffset = timeZone.getRawOffset();
        this.precision = 6;
    }

    public ZonedTimestampColumn(long data, TimeZone timeZone, int precision) {
        super(data, 16);
        this.nanos = (int) (data % 1000 * 1000000);
        this.millisecondOffset = timeZone.getRawOffset();
        this.precision = precision;
    }

    public ZonedTimestampColumn(long data, ZoneId zoneId) {
        super(data, 16);
        this.nanos = (int) (data % 1000 * 1000000);
        this.millisecondOffset = TimeZone.getTimeZone(zoneId).getRawOffset();
        this.precision = 6;
    }

    public ZonedTimestampColumn(long data, ZoneId zoneId, int precision) {
        super(data, 16);
        this.nanos = (int) (data % 1000 * 1000000);
        this.millisecondOffset = TimeZone.getTimeZone(zoneId).getRawOffset();
        this.precision = precision;
    }

    public ZonedTimestampColumn(long data, int nanos, int millisecondOffset, int precision) {
        super(data, 16);
        this.nanos = nanos;
        this.millisecondOffset = millisecondOffset;
        this.precision = precision;
    }

    private ZonedTimestampColumn(
            long data, int nanos, int millisecondOffset, int precision, int byteSize) {
        super(data, byteSize);
        this.nanos = nanos;
        this.millisecondOffset = millisecondOffset;
        this.precision = precision;
    }

    public static ZonedTimestampColumn from(
            long data, int nanos, int millisecondOffset, int precision) {
        return new ZonedTimestampColumn(data, nanos, millisecondOffset, precision, 0);
    }

    @Override
    public String type() {
        return "ZONEDTIMESTAMP";
    }

    @Override
    public Boolean asBooleanInternal() {
        throw new CastException("Timestamp", "Boolean", this.asStringInternal());
    }

    @Override
    public byte[] asBytesInternal() {
        throw new CastException("Timestamp", "Bytes", this.asStringInternal());
    }

    @Override
    public String asStringInternal() {
        return asTimestampStrInternal();
    }

    @Override
    public String asTimestampStrInternal() {
        return generateTimeStrWithMilliOffset(millisecondOffset);
    }

    public String asTimestampStrWithTimeZone(TimeZone timeZone) {
        return generateTimeStrWithMilliOffset(timeZone.getRawOffset());
    }

    private String generateTimeStrWithMilliOffset(int offset) {
        //  +hh:mm or -hh:mm
        String formattedOffset =
                (offset < 0)
                        ? String.format(
                                Locale.US,
                                "-%1$02d:%2$02d",
                                -offset / 3600000,
                                -offset % 3600000 / 60000)
                        : String.format(
                                Locale.US,
                                "+%1$02d:%2$02d",
                                offset / 3600000,
                                offset % 3600000 / 60000);

        Calendar calendar =
                Calendar.getInstance(TimeZone.getTimeZone("GMT" + formattedOffset), Locale.US);
        long milliSecond = (Long) data;
        calendar.setTimeInMillis(milliSecond);
        String res;
        if (precision <= 0) {
            res = String.format(Locale.US, "%1$tF %1$tT %2$s", calendar, formattedOffset);
        } else if (precision <= 3) {
            res =
                    String.format(
                            Locale.US,
                            "%1$tF %1$tT.%2$s %3$s",
                            calendar,
                            BigDecimal.valueOf(milliSecond % 1000 * 1000000, 9)
                                    .toPlainString()
                                    .substring(2, 2 + precision),
                            formattedOffset);
        } else {
            res =
                    String.format(
                            Locale.US,
                            "%1$tF %1$tT.%2$s %3$s",
                            calendar,
                            BigDecimal.valueOf(nanos, 9) // -> 0.123456000
                                    .toPlainString()
                                    .substring(2, 2 + precision), // -> "123456"
                            formattedOffset);
        }
        return res;
    }

    @Override
    public BigDecimal asBigDecimalInternal() {
        return new BigDecimal(asTimestampInternal().getTime());
    }

    @Override
    public Long asLong() {
        if (null == data) {
            return null;
        }
        return asTimestampInternal().getTime();
    }

    public Timestamp asTimestampWithUtc() {
        Timestamp timestamp = new Timestamp((long) data);
        if (precision > 3) {
            timestamp.setNanos(nanos);
        }
        return timestamp;
    }

    @Override
    public Timestamp asTimestampInternal() {
        Timestamp timestamp =
                new Timestamp((long) data - defaultMillisecondOffset + millisecondOffset);
        if (precision > 3) {
            timestamp.setNanos(nanos);
        }
        return timestamp;
    }

    public Timestamp asTimestampWithTimeZone(TimeZone timeZone) {
        if (data == null) {
            return null;
        }
        Timestamp timestamp =
                new Timestamp((long) data - defaultMillisecondOffset + timeZone.getRawOffset());
        if (precision > 3) {
            timestamp.setNanos(nanos);
        }
        return timestamp;
    }

    @Override
    public ZonedTimestampColumn asZonedTimestamp() {
        return this;
    }

    @Override
    public Time asTimeInternal() {
        return new Time(asTimestampInternal().getTime());
    }

    @Override
    public java.sql.Date asSqlDateInternal() {
        return java.sql.Date.valueOf(asTimestamp().toLocalDateTime().toLocalDate());
    }

    @Override
    public Integer asYearInt() {
        if (null == data) {
            return null;
        }
        return asTimestampInternal().toLocalDateTime().getYear();
    }

    @Override
    public Integer asMonthInt() {
        if (null == data) {
            return null;
        }
        return asTimestampInternal().toLocalDateTime().getMonthValue();
    }

    public int getPrecision() {
        return precision;
    }

    public int getNanos() {
        return nanos;
    }

    public int getMillisecondOffset() {
        return millisecondOffset;
    }
}
