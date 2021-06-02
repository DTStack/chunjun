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
import com.dtstack.flinkx.util.DateUtil;
import org.apache.commons.lang3.math.NumberUtils;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Date: 2021/04/26
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class StringColumn extends AbstractBaseColumn {

    private String format;

    public StringColumn(String data) {
        super(data);
    }

    public StringColumn(final String data, String format) {
        super(data);
        this.format = format;
    }

    @Override
    public int getByteSize(Object data) {
        return null == data ? 0 : ((String)data).getBytes(StandardCharsets.UTF_8).length;
    }

    @Override
    public String asString() {
        if (null == data) {
            return null;
        }
        return (String) data;
    }

    @Override
    public Date asDate() {
        try {
            if (null == this.asString()) {
                return null;
            }

            Long time = null;
            try {
                //如果string是时间戳
                time = NumberUtils.createLong((String) getData());
            } catch (UnsupportedOperationException e) {
                //doNothing
            }
            SimpleDateFormat formatter = DateUtil.buildDateFormatter(format);
            if (time != null) {
                Date date = new Date(time);
                return formatter.parse(formatter.format(date));
            } else if (formatter != null) {
                try {
                    //如果是日期格式字符串
                    return formatter.parse(this.asString());
                } catch (ParseException ignored) {
                }
            }
            return DateUtil.columnToDate(this.asString(), formatter);
        } catch (Exception e) {
            throw new RuntimeException(String.format("String[\"%s\"]can not cast to Date.", this.asString()));
        }
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
        if (null == this.getData()) {
            return null;
        }

        //如果是数值类型
        try {
            return NumberUtils.toInt((String) getData()) != 0;
        } catch (Exception ignored) {

        }

        if ("true".equalsIgnoreCase(this.asString())) {
            return true;
        }

        if ("false".equalsIgnoreCase(this.asString())) {
            return false;
        }

        throw new RuntimeException(String.format("String[\"%s\"] can not cast to Boolean.", this.asString()));
    }

    @Override
    public BigDecimal asBigDecimal() {
        if (null == this.getData()) {
            return null;
        }

        this.validateDoubleSpecific((String) this.getData());

        try {
            return new BigDecimal(this.asString());
        } catch (Exception e) {
            throw new RuntimeException(String.format("String [\"%s\"]can not cast to BigDecimal.", this.asString()));
        }
    }

    @Override
    public Timestamp asTimestamp() {
        //todo
        throw new UnsupportedOperationException(String.format("String [\"%s\"]can not cast to Timestamp", this.asString()));
    }

    private void validateDoubleSpecific(final String data) {
        if ("NaN".equals(data) || "Infinity".equals(data)
                || "-Infinity".equals(data)) {
            throw new RuntimeException(String.format("String[\"%s\"]属于Double特殊类型，不能转为其他类型.", data));
        }
    }
}
