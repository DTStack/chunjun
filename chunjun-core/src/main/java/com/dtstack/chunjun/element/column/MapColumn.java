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
import com.dtstack.chunjun.util.JsonUtil;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;

/**
 * Date: 2021/04/26 Company: www.dtstack.com
 *
 * @author tudou
 */
public class MapColumn extends AbstractBaseColumn {

    public MapColumn(Map<String, Object> data) {
        super(data, 0);
        if (data != null) {
            byteSize += data.toString().length();
        }
    }

    private MapColumn(Map<String, Object> data, int byteSize) {
        super(data, byteSize);
    }

    public static MapColumn from(Map<String, Object> data) {
        return new MapColumn(data, 0);
    }

    @Override
    public String asStringInternal() {
        return JsonUtil.toJson(data);
    }

    @Override
    public Date asDate() {
        if (null == data) {
            return null;
        }
        throw new CastException("Map", "Date", this.asString());
    }

    @Override
    public byte[] asBytesInternal() {
        return JsonUtil.toJson(data).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String type() {
        return "MAP";
    }

    @Override
    public Boolean asBooleanInternal() {
        throw new CastException("Map", "Boolean", this.asStringInternal());
    }

    @Override
    public BigDecimal asBigDecimalInternal() {
        throw new CastException("Map", "BigDecimal", this.asStringInternal());
    }

    @Override
    public Timestamp asTimestampInternal() {
        throw new CastException("Map", "Timestamp", this.asStringInternal());
    }

    @Override
    public Time asTimeInternal() {
        throw new CastException("Map", "java.sql.Time", this.asStringInternal());
    }

    @Override
    public java.sql.Date asSqlDateInternal() {
        throw new CastException("Map", "java.sql.Date", this.asStringInternal());
    }

    @Override
    public String asTimestampStrInternal() {
        throw new CastException("Map", "Timestamp", this.asStringInternal());
    }
}
