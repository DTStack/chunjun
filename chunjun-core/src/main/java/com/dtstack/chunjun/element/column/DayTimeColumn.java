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

package com.dtstack.chunjun.element.column;

import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.throwable.CastException;

import org.apache.flink.table.types.logical.DayTimeIntervalType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public class DayTimeColumn extends AbstractBaseColumn {

    private int nanos;

    private DayTimeFormat format;

    public DayTimeColumn(long millisecond) {
        super(millisecond, 8);
    }

    public DayTimeColumn(long millisecond, int nanos) {
        super(millisecond, 12);
        this.nanos = nanos;
    }

    private DayTimeColumn(long millisecond, int nanos, int byteSize) {
        super(millisecond, byteSize);
        this.nanos = nanos;
    }

    public static DayTimeColumn from(long millisecond, int nanos) {
        return new DayTimeColumn(millisecond, nanos, 0);
    }

    @Override
    public String type() {
        return "DAY-SECOND";
    }

    @Override
    public Boolean asBooleanInternal() {
        throw new CastException("day-second", "boolean", this.asStringInternal());
    }

    @Override
    public byte[] asBytesInternal() {
        throw new CastException("day-second", "byte", this.asStringInternal());
    }

    @Override
    public String asStringInternal() {
        if (format == null) {
            return String.valueOf(data);
        }
        return format.asString((Long) data, nanos);
    }

    @Override
    public BigDecimal asBigDecimalInternal() {
        return new BigDecimal((long) data);
    }

    @Override
    public Timestamp asTimestampInternal() {
        return format.asTimestamp((Long) data, nanos);
    }

    @Override
    public Time asTimeInternal() {
        return new Time((long) data);
    }

    @Override
    public Date asSqlDateInternal() {
        return new Date((long) data);
    }

    @Override
    public String asTimestampStrInternal() {
        return asTimestampInternal().toString();
    }

    public void setDayTimeFormat(DayTimeFormat format) {
        this.format = format;
    }

    public int getNanos() {
        return nanos;
    }

    public static void main(String[] args) {
        //        DayTimeColumn daySecondColumn = new DayTimeColumn(System.currentTimeMillis(),
        // 3, 0);
        //        daySecondColumn.setFormat(
        //                getDayTimeFormat(
        //                        new DayTimeIntervalType(
        //                                DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND, 5,
        // 3)));
        //        System.out.println(daySecondColumn.asStringInternal());
    }

    public static DayTimeFormat getDayTimeFormat(DayTimeIntervalType type) {
        switch (type.getResolution()) {
            case DAY:
                return new Day(type);
            case DAY_TO_HOUR:
                return new DayToHour(type);
            case DAY_TO_MINUTE:
                return new DayToMinute(type);
            case DAY_TO_SECOND:
                return new DayToSecond(type);
            case HOUR:
                return new Hour(type);
            case HOUR_TO_MINUTE:
                return new HourToMinute(type);
            case HOUR_TO_SECOND:
                return new HourToSecond(type);
            case MINUTE:
                return new Minute(type);
            case MINUTE_TO_SECOND:
                return new MinuteToSecond(type);
            case SECOND:
                return new Second(type);
            default:
                throw new IllegalArgumentException(
                        "Unsupported DayTimeIntervalType: " + type.getResolution());
        }
    }

    static class Day extends DayTimeFormat {
        protected Day(DayTimeIntervalType dayTimeIntervalType) {
            super(dayTimeIntervalType);
        }

        @Override
        String asString(long millisecond, int nanos) {
            return getDayString(millisecond, dayPrecision);
        }

        @Override
        Timestamp asTimestamp(long millisecond, int nanos) {
            return new Timestamp(millisecond);
        }
    }

    static class DayToHour extends DayTimeFormat {
        protected DayToHour(DayTimeIntervalType dayTimeIntervalType) {
            super(dayTimeIntervalType);
        }

        @Override
        String asString(long millisecond, int nanos) {
            int hour = (int) ((millisecond % 86400000) / 3600000);
            return String.format("%s %s", getDayString(millisecond, dayPrecision), hour);
        }

        @Override
        Timestamp asTimestamp(long millisecond, int nanos) {
            return new Timestamp(millisecond);
        }
    }

    static class DayToMinute extends DayTimeFormat {
        protected DayToMinute(DayTimeIntervalType dayTimeIntervalType) {
            super(dayTimeIntervalType);
        }

        @Override
        String asString(long millisecond, int nanos) {
            int hour = (int) ((millisecond % 86400000) / 3600000);
            int minute = (int) ((millisecond % 3600000) / 60000);
            return String.format("%s %s:%s", getDayString(millisecond, dayPrecision), hour, minute);
        }

        @Override
        Timestamp asTimestamp(long millisecond, int nanos) {
            return new Timestamp(millisecond);
        }
    }

    static class DayToSecond extends DayTimeFormat {
        protected DayToSecond(DayTimeIntervalType dayTimeIntervalType) {
            super(dayTimeIntervalType);
        }

        @Override
        String asString(long millisecond, int nanos) {
            int hour = (int) ((millisecond % 86400000) / 3600000);
            int minute = (int) ((millisecond % 3600000) / 60000);
            return String.format(
                    "%s %s:%s:%s",
                    getDayString(millisecond, dayPrecision),
                    hour,
                    minute,
                    getSecondString(millisecond, nanos, fractionalPrecision));
        }

        @Override
        Timestamp asTimestamp(long millisecond, int nanos) {
            Timestamp timestamp = new Timestamp(millisecond);
            if (fractionalPrecision > 3) {
                timestamp.setNanos(nanos);
            }
            return timestamp;
        }
    }

    static class Hour extends DayTimeFormat {
        protected Hour(DayTimeIntervalType dayTimeIntervalType) {
            super(dayTimeIntervalType);
        }

        @Override
        String asString(long millisecond, int nanos) {
            int hour = (int) (millisecond / 3600000);
            return String.valueOf(hour);
        }

        @Override
        Timestamp asTimestamp(long millisecond, int nanos) {
            return new Timestamp(millisecond);
        }
    }

    static class HourToMinute extends DayTimeFormat {
        protected HourToMinute(DayTimeIntervalType dayTimeIntervalType) {
            super(dayTimeIntervalType);
        }

        @Override
        String asString(long millisecond, int nanos) {
            int hour = (int) (millisecond / 3600000);
            int minute = (int) ((millisecond % 3600000) / 60000);
            return String.format("%s:%s", hour, minute);
        }

        @Override
        Timestamp asTimestamp(long millisecond, int nanos) {
            return new Timestamp(millisecond);
        }
    }

    static class HourToSecond extends DayTimeFormat {
        protected HourToSecond(DayTimeIntervalType dayTimeIntervalType) {
            super(dayTimeIntervalType);
        }

        @Override
        String asString(long millisecond, int nanos) {
            int hour = (int) (millisecond / 3600000);
            int minute = (int) ((millisecond % 3600000) / 60000);
            return String.format(
                    "%s:%s:%s",
                    hour, minute, getSecondString(millisecond, nanos, fractionalPrecision));
        }

        @Override
        Timestamp asTimestamp(long millisecond, int nanos) {
            Timestamp timestamp = new Timestamp(millisecond);
            if (fractionalPrecision > 3) {
                timestamp.setNanos(nanos);
            }
            return timestamp;
        }
    }

    static class Minute extends DayTimeFormat {
        protected Minute(DayTimeIntervalType dayTimeIntervalType) {
            super(dayTimeIntervalType);
        }

        @Override
        String asString(long millisecond, int nanos) {
            int minute = (int) (millisecond / 60000);
            return String.valueOf(minute);
        }

        @Override
        Timestamp asTimestamp(long millisecond, int nanos) {
            return new Timestamp(millisecond);
        }
    }

    static class MinuteToSecond extends DayTimeFormat {
        protected MinuteToSecond(DayTimeIntervalType dayTimeIntervalType) {
            super(dayTimeIntervalType);
        }

        @Override
        String asString(long millisecond, int nanos) {
            int minute = (int) (millisecond / 60000);
            return String.format(
                    "%s:%s", minute, getSecondString(millisecond, nanos, fractionalPrecision));
        }

        @Override
        Timestamp asTimestamp(long millisecond, int nanos) {
            return new Timestamp(millisecond);
        }
    }

    static class Second extends DayTimeFormat {
        protected Second(DayTimeIntervalType dayTimeIntervalType) {
            super(dayTimeIntervalType);
        }

        @Override
        String asString(long millisecond, int nanos) {
            return getSecondString(millisecond, nanos, fractionalPrecision);
        }

        @Override
        Timestamp asTimestamp(long millisecond, int nanos) {
            Timestamp timestamp = new Timestamp(millisecond);
            if (fractionalPrecision > 3) {
                timestamp.setNanos(nanos);
            }
            return timestamp;
        }
    }

    private static String getDayString(long millisecond, int dayPrecision) {
        String dayStr = String.valueOf(millisecond / 86400000);
        dayStr = dayStr.substring(Math.max(0, dayStr.length() - dayPrecision));
        return dayStr;
    }

    private static String getSecondString(long millisecond, int nanos, int fractionalPrecision) {
        millisecond = millisecond % 60000;
        if (fractionalPrecision <= 0) {
            return String.valueOf(millisecond / 1000);
        } else if (fractionalPrecision <= 3) {
            return String.format(
                    "%s.%s",
                    millisecond / 1000,
                    BigDecimal.valueOf(millisecond % 1000 * 1000000, 9)
                            .toPlainString()
                            .substring(2, 2 + fractionalPrecision));
        } else {
            return String.format(
                    "%s.%s",
                    millisecond / 1000,
                    BigDecimal.valueOf(nanos, 9)
                            .toPlainString()
                            .substring(2, 2 + fractionalPrecision));
        }
    }

    public abstract static class DayTimeFormat implements Serializable {

        private static final long serialVersionUID = 1L;
        protected final int dayPrecision;
        protected final int fractionalPrecision;

        protected DayTimeFormat(DayTimeIntervalType dayTimeIntervalType) {
            this.dayPrecision = dayTimeIntervalType.getDayPrecision();
            this.fractionalPrecision = dayTimeIntervalType.getFractionalPrecision();
        }

        abstract String asString(long millisecond, int nanos);

        abstract Timestamp asTimestamp(long millisecond, int nanos);
    }
}
