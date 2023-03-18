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

import org.apache.flink.table.types.logical.YearMonthIntervalType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class YearMonthColumn extends AbstractBaseColumn {

    YearMonthFormat yearMonthFormat;

    public YearMonthColumn(int month) {
        super(month, 8);
    }

    public YearMonthColumn(int month, int size) {
        super(month, size);
    }

    public static YearMonthColumn from(int month) {
        return new YearMonthColumn(month, 0);
    }

    @Override
    public String type() {
        return "YEAR-MONTH";
    }

    @Override
    public Boolean asBooleanInternal() {
        throw new CastException("year-month", "boolean", this.asStringInternal());
    }

    @Override
    public byte[] asBytesInternal() {
        throw new CastException("year-month", "byte[]", this.asStringInternal());
    }

    @Override
    public String asStringInternal() {
        if (yearMonthFormat == null) {
            return String.valueOf(data);
        }
        return yearMonthFormat.asString((Integer) data);
    }

    @Override
    public BigDecimal asBigDecimalInternal() {
        return new BigDecimal((Integer) data);
    }

    @Override
    public Timestamp asTimestampInternal() {
        return Timestamp.valueOf(LocalDateTime.of(asYearInt(), asMonthInt(), 1, 0, 0));
    }

    @Override
    public Time asTimeInternal() {
        throw new CastException("year-month", "java.sql.Time", this.asStringInternal());
    }

    @Override
    public Date asSqlDateInternal() {
        return Date.valueOf(LocalDate.of(asYearInt(), asMonthInt(), 1));
    }

    @Override
    public String asTimestampStrInternal() {
        return asTimestampInternal().toString();
    }

    @Override
    public Integer asYearInt() {
        if (data == null) {
            return null;
        }
        return (int) data / 12;
    }

    @Override
    public Integer asMonthInt() {
        if (null == data) {
            return null;
        }
        int monthValue = this.asInt();
        if (monthValue % 12 == 0) {
            return 12;
        } else {
            return monthValue % 12;
        }
    }

    public void setYearMonthFormat(YearMonthFormat yearMonthFormat) {
        this.yearMonthFormat = yearMonthFormat;
    }

    @Override
    public String toString() {
        return String.valueOf(data);
    }

    public static YearMonthFormat getYearMonthFormat(YearMonthIntervalType yearMonthIntervalType) {
        switch (yearMonthIntervalType.getResolution()) {
            case YEAR:
                return new Year(yearMonthIntervalType);
            case YEAR_TO_MONTH:
                return new YearMonth(yearMonthIntervalType);
            case MONTH:
                return new Month(yearMonthIntervalType);
            default:
                throw new IllegalArgumentException(
                        "Unsupported YearMonthIntervalType: " + yearMonthIntervalType);
        }
    }

    public abstract static class YearMonthFormat implements Serializable {

        private static final long serialVersionUID = 1L;
        protected final int yearPrecision;

        protected YearMonthFormat(YearMonthIntervalType yearMonthIntervalType) {
            this.yearPrecision = yearMonthIntervalType.getYearPrecision();
        }

        abstract String asString(int month);
    }

    static class Year extends YearMonthFormat {

        Year(YearMonthIntervalType yearMonthIntervalType) {
            super(yearMonthIntervalType);
        }

        @Override
        String asString(int month) {
            String yearStr = String.valueOf(month / 12);
            return yearStr.substring(Math.max(0, yearStr.length() - yearPrecision));
        }
    }

    static class YearMonth extends YearMonthFormat {
        protected YearMonth(YearMonthIntervalType yearMonthIntervalType) {
            super(yearMonthIntervalType);
        }

        @Override
        String asString(int month) {
            int yearInt;
            int monthData;
            if (month % 12 == 0) {
                yearInt = month / 12 - 1;
                monthData = 12;
            } else {
                yearInt = month / 12;
                monthData = month % 12;
            }
            String yearStr = String.valueOf(yearInt);
            yearStr.substring(Math.max(0, yearStr.length() - yearPrecision));
            return yearInt + "-" + monthData;
        }
    }

    static class Month extends YearMonthFormat {
        protected Month(YearMonthIntervalType yearMonthIntervalType) {
            super(yearMonthIntervalType);
        }

        @Override
        String asString(int month) {
            return String.valueOf(month);
        }
    }
}
