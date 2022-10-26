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

package com.dtstack.chunjun.restore.mysql.transformer;

import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.BytesColumn;
import com.dtstack.chunjun.element.column.NullColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimeColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.restore.mysql.util.TransformerUtil;

import com.alibaba.fastjson.JSONObject;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public class ColumnTransformer {

    private static final DateTimeFormatter TIME_WITH_AM_OR_PM_FORMATTER =
            DateTimeFormatter.ofPattern("hh:mm:ss a", Locale.ENGLISH);

    private ColumnTransformer() throws IllegalAccessException {
        throw new IllegalAccessException(getClass() + " can not be instantiated.");
    }

    public static AbstractBaseColumn transform(JSONObject jsonObject) {
        String type = jsonObject.getString("type");
        switch (type) {
            case "STRING":
                return new StringColumn(
                        jsonObject.getString("data"),
                        jsonObject.getString("format"),
                        jsonObject.getBoolean("isCustomFormat"),
                        jsonObject.getIntValue("byteSize"));
            case "BIGDECIMAL":
                return new BigDecimalColumn(
                        jsonObject.getBigDecimal("data"), jsonObject.getIntValue("byteSize"));
            case "TIMESTAMP":
                return new TimestampColumn(
                        Timestamp.valueOf(jsonObject.getString("data")),
                        jsonObject.getIntValue("precision"));
            case "BYTES":
                return new BytesColumn(
                        TransformerUtil.objectToBytes(jsonObject.getJSONArray("data").toArray()),
                        jsonObject.getIntValue("byteSize"),
                        jsonObject.getString("encoding"));
            case "BOOLEAN":
                return new BooleanColumn(
                        jsonObject.getBooleanValue("data"), jsonObject.getIntValue("byteSize"));
            case "NULL":
                return new NullColumn();
            case "TIME":
                LocalTime localTime =
                        LocalTime.parse(jsonObject.getString("data"), TIME_WITH_AM_OR_PM_FORMATTER);
                return new TimeColumn(Time.valueOf(localTime));
        }

        throw new IllegalArgumentException("Can not transform type: " + type);
    }
}
