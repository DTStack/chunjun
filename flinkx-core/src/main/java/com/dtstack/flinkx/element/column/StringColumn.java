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
package com.dtstack.flinkx.element.column;

import com.dtstack.flinkx.element.AbstractBaseColumn;
import com.dtstack.flinkx.throwable.CastException;
import com.dtstack.flinkx.util.DateUtil;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Date: 2021/04/26 Company: www.dtstack.com
 *
 * @author tudou
 */
public class StringColumn extends AbstractBaseColumn {

    private SimpleDateFormat format;
    private boolean isCustomFormat = false;

    public StringColumn(final String data) {
        super(data);
        this.format = DateUtil.buildDateFormatter("yyyy-MM-dd HH:mm:ss");
    }

    public StringColumn(final String data, String format) {
        super(data);
        if (StringUtils.isNotBlank(format)) {
            this.format = DateUtil.buildDateFormatter(format);
            isCustomFormat = true;
        }
    }

    public StringColumn(Byte aByte) {
        super(aByte);
    }

    @Override
    public String asString() {
        if (null == data) {
            return null;
        }
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
        String data = this.asString();
        try {
            // 如果string是时间戳
            time = NumberUtils.createLong(data);
        } catch (Exception ignored) {
            // doNothing
        }
        if (time != null) {
            Date date = new Date(time);
            try {
                result = format.parse(format.format(date));
            } catch (ParseException ignored) {
                // doNothing
            }
        } else {
            try {
                // 如果是日期格式字符串
                result = format.parse(data);
            } catch (ParseException ignored) {
                // doNothing
            }
        }

        if (result == null) {
            result = DateUtil.columnToDate(data, format);

            if (result == null) {
                throw new CastException("String", "Date", data);
            }
        }

        return result;
    }

    @Override
    public byte[] asBytes() {
        if (null == data) {
            return null;
        }
        return ((String) data).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Boolean asBoolean() {
        if (null == data) {
            return null;
        }

        String data = this.asString();
        // 如果是数值类型
        try {
            return NumberUtils.toInt(data) != 0;
        } catch (Exception ignored) {
            // doNothing
        }

        if ("true".equalsIgnoreCase(data)) {
            return true;
        }

        if ("false".equalsIgnoreCase(data)) {
            return false;
        }

        throw new CastException("String", "Boolean", data);
    }

    @Override
    public BigDecimal asBigDecimal() {
        if (null == data) {
            return null;
        }
        String data = this.asString();
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

        String data = this.asString();
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
    public Timestamp asTimestamp() {
        if (null == data) {
            return null;
        }
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
    public Time asTime() {
        if (null == data) {
            return null;
        }
        throw new CastException("String", "java.sql.Time", this.asString());
    }

    @Override
    public java.sql.Date asSqlDate() {
        if (null == data) {
            return null;
        }
        return java.sql.Date.valueOf(asTimestamp().toLocalDateTime().toLocalDate());
    }

    @Override
    public String asTimestampStr() {
        if (null == data) {
            return null;
        }
        String data = this.asString();
        try {
            // 如果string是时间戳
            Long time = NumberUtils.createLong(data);
            return format.format(time);
        } catch (Exception ignored) {
            // doNothing
        }

        try {
            if (isCustomFormat) {
                // 格式化
                return format.format(asDate().getTime());
            } else {
                // 校验格式
                DateUtil.stringToDate(data);
                return data;
            }
        } catch (Exception e) {
            throw new CastException("String", "Timestamp", data);
        }
    }
}
