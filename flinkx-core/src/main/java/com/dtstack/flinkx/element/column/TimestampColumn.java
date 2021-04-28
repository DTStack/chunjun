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

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;

/**
 * Date: 2021/04/27
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class TimestampColumn extends AbstractBaseColumn {

    public TimestampColumn(Timestamp data) {
        super(data);
    }

    public TimestampColumn(long data) {
        super(new Timestamp(data));
    }

    @Override
    public int getByteSize(Object data) {
        return null == data ? 0 : data.toString().getBytes(StandardCharsets.UTF_8).length;
    }

    @Override
    public Boolean asBoolean() {
        throw new RuntimeException(String.format("Timestamp[\"%s\"]can not cast to Boolean.", this.asString()));
    }

    @Override
    public byte[] asBytes() {
        throw new RuntimeException(String.format("Timestamp[\"%s\"]can not cast to byte[].", this.asString()));
    }

    @Override
    public String asString() {
        if (null == data) {
            return null;
        }
        return data.toString();
    }

    @Override
    public BigDecimal asBigDecimal() {
        throw new RuntimeException(String.format("Timestamp[\"%s\"]can not cast to BigDecimal.", this.asString()));
    }

    @Override
    public Timestamp asTimestamp() {
        if (null == data) {
            return null;
        }
        return (Timestamp) data;
    }
}
