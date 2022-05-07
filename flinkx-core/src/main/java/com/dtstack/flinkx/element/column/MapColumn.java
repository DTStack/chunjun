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
import com.dtstack.flinkx.util.JsonUtil;

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
        super(data);
    }

    @Override
    public String asString() {
        if (null == data) {
            return null;
        }
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
    public byte[] asBytes() {
        if (null == data) {
            return null;
        }
        return JsonUtil.toJson(data).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Boolean asBoolean() {
        if (null == data) {
            return null;
        }
        throw new CastException("Map", "Boolean", this.asString());
    }

    @Override
    public BigDecimal asBigDecimal() {
        if (null == data) {
            return null;
        }
        throw new CastException("Map", "BigDecimal", this.asString());
    }

    @Override
    public Timestamp asTimestamp() {
        if (null == data) {
            return null;
        }
        throw new CastException("Map", "Timestamp", this.asString());
    }

    @Override
    public Time asTime() {
        if (null == data) {
            return null;
        }
        throw new CastException("Map", "java.sql.Time", this.asString());
    }

    @Override
    public java.sql.Date asSqlDate() {
        if (null == data) {
            return null;
        }
        throw new CastException("Map", "java.sql.Date", this.asString());
    }

    @Override
    public String asTimestampStr() {
        if (null == data) {
            return null;
        }
        throw new CastException("Map", "Timestamp", this.asString());
    }
}
