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
import com.dtstack.chunjun.util.DateUtil;

import org.apache.flink.table.types.logical.LogicalType;

import cn.hutool.core.date.LocalDateTimeUtil;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

import static com.dtstack.chunjun.element.ClassSizeUtil.getStringSize;

/**
 * Date: 2021/04/26 Company: www.dtstack.com
 *
 * @author tudou
 */
public class StringColumn extends AbstractBaseColumn {

    private String format = "yyyy-MM-dd HH:mm:ss";
    private boolean isCustomFormat = false;

    public StringColumn(final String data) {
        super(data, 0);
        byteSize += getStringSize(data);
    }

    public StringColumn(final String data, String format) {
        super(data, 0);
        if (StringUtils.isNotBlank(format)) {
            this.format = format;
            isCustomFormat = true;
        }
        byteSize += getStringSize(data);
    }

    public StringColumn(String data, String format, boolean isCustomFormat, int byteSize) {
        super(data, byteSize);
        this.format = format;
        this.isCustomFormat = isCustomFormat;
    }

    public static StringColumn from(final String data, String format, boolean isCustomFormat) {
        return new StringColumn(data, format, isCustomFormat, 0);
    }

    public StringColumn(Byte aByte) {
        super(aByte, 0);
        byteSize += 1;
    }

    @Override
    public String asStringInternal() {
        if (isCustomFormat) {
            return asTimestampStr();
        } else {
            return String.valueOf(data);
        }
    }

    @Override
    public Date asDate() {
        if (null == data) {
            return null;
        }
        Long time = null;
        Date result = null;
        String data = (String) this.data;
        try {
            // 如果string是时间戳
            time = NumberUtils.createLong(data);
        } catch (Exception ignored) {
            // doNothing
        }
        if (time != null) {
            try {
                result =
                        new Date(
                                DateUtil.columnToTimestamp(
                                                LocalDateTimeUtil.format(
                                                        new Timestamp(time).toLocalDateTime(),
                                                        format),
                                                format)
                                        .getTime());
            } catch (Exception ignored) {
                // doNothing
            }
        } else {
            try {
                // 如果是日期格式字符串
                result = new Date(DateUtil.columnToTimestamp(data, format).getTime());
            } catch (Exception ignored) {
                // doNothing
            }
        }

        if (result == null) {
            result = DateUtil.columnToDate(data, null);

            if (result == null) {
                throw new CastException("String", "Date", data);
            }
        }

        return result;
    }

    @Override
    public byte[] asBytesInternal() {
        return ((String) data).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String type() {
        return "STRING";
    }

    @Override
    public Boolean asBooleanInternal() {
        if (null == data) {
            return null;
        }

        String data = String.valueOf(this.data);

        if ("true".equalsIgnoreCase(data)) {
            return true;
        }

        if ("false".equalsIgnoreCase(data)) {
            return false;
        }

        // 如果是数值类型
        try {
            return NumberUtils.toInt(data) != 0;
        } catch (Exception ignored) {
            // doNothing
        }

        throw new CastException("String", "Boolean", data);
    }

    @Override
    public BigDecimal asBigDecimalInternal() {
        String data = String.valueOf(this.data);
        this.validateDoubleSpecific(data);

        try {
            return new BigDecimal(data);
        } catch (Exception e) {
            throw new CastException("String", "BigDecimal", data);
        }
    }

    @Override
    public Double asDouble() {
        if (null == data) {
            return null;
        }

        String data = String.valueOf(this.data);
        if ("NaN".equals(data)) {
            return Double.NaN;
        }

        if ("Infinity".equals(data)) {
            return Double.POSITIVE_INFINITY;
        }

        if ("-Infinity".equals(data)) {
            return Double.NEGATIVE_INFINITY;
        }

        return super.asDouble();
    }

    @Override
    public Timestamp asTimestampInternal() {
        try {
            return new Timestamp(this.asDate().getTime());
        } catch (CastException e) {
            throw new CastException("String", "Timestamp", (String) data);
        }
    }

    private void validateDoubleSpecific(final String data) {
        if ("NaN".equals(data) || "Infinity".equals(data) || "-Infinity".equals(data)) {
            throw new CastException(
                    String.format(
                            "String[%s]belongs to the special type of Double and cannot be converted to other types.",
                            data));
        }
    }

    @Override
    public Time asTimeInternal() {
        return new Time(asTimestamp().getTime());
    }

    @Override
    public java.sql.Date asSqlDateInternal() {
        return java.sql.Date.valueOf(asTimestamp().toLocalDateTime().toLocalDate());
    }

    @Override
    public String asTimestampStrInternal() {
        String data = String.valueOf(this.data);
        try {
            // 如果string是时间戳
            Long time = NumberUtils.createLong(data);
            return LocalDateTimeUtil.format(new Timestamp(time).toLocalDateTime(), format);
        } catch (Exception ignored) {
            // doNothing
        }

        try {
            if (isCustomFormat) {
                // 格式化
                return LocalDateTimeUtil.format(
                        new Timestamp(asDate().getTime()).toLocalDateTime(), format);
            } else {
                // 校验格式
                DateUtil.stringToDate(data);
                return data;
            }
        } catch (Exception e) {
            throw new CastException("String", "Timestamp", data);
        }
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
        return asTimestampInternal().toLocalDateTime().getYear();
    }

    @Override
    public Object asArray(LogicalType elementType) {
        if (null == data) {
            return null;
        }
        try {
            return JSON.parseObject((String) data, Object[].class);
        } catch (Exception e) {
            throw new CastException("String", "ARRAY", asStringInternal());
        }
    }
    //
    //    @Override
    //    public Map<?, ?> asBaseMap() {
    //        if (null == data) {
    //            return null;
    //        }
    //        try {
    //            return JSON.parseObject((String) data, Map.class);
    //        } catch (Exception e) {
    //            throw new CastException("String", "MAP", asStringInternal());
    //        }
    //    }

    public boolean isCustomFormat() {
        return isCustomFormat;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }
}
