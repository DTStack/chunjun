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
package com.dtstack.flinkx.restapi.common;

import org.apache.commons.lang3.StringUtils;

import java.sql.Date;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * ConstantParan
 *
 * @author by dujie@dtstack.com
 * @Date 2020/9/26
 */
public class ConstantParam<T> implements ParamDefinition {

    private final String name;

    private final ParamType paramType;

    private final Class<T> valueClass;

    protected Object value;

    private final String description;

    private String formatDescription;

    private final DateTimeFormatter format;

    public ConstantParam(String name, ParamType paramType, Class valueClass, Object value, String description, String format) {
        this.name = name;
        this.paramType = paramType;
        this.valueClass = valueClass;
        this.description = description;
        this.value = value;
        this.formatDescription = format;
        if (StringUtils.isNotBlank(format)) {
            this.format = DateTimeFormatter.ofPattern(format);
        } else {
            this.format = null;
        }

    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public ParamType getType() {
        return paramType;
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public String getValueType() {
        return "valueClass";
    }


    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public String getFormat() {
        return formatDescription;
    }

    @Override
    public Object format(Object data) {
        if (Objects.isNull(format) || Objects.isNull(data)) {
            return data;
        }
        if (getValueType().equals(Date.class)) {
            Date value1 = (Date) data;
            LocalDateTime ldt = value1.toInstant()
                    .atZone(ZoneId.systemDefault())
                    .toLocalDateTime();
            return format.format(ldt);
        }
        return null;
    }


}
