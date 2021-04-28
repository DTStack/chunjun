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
import java.sql.Timestamp;

/**
 * Date: 2021/04/27
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class BooleanColumn extends AbstractBaseColumn {

    public BooleanColumn(boolean data) {
        super(data);
    }

    public BooleanColumn(int data) {
        super(new byte[data]);
    }

    @Override
    public int getByteSize(Object data) {
        return null == data ? 0 : 1;
    }

    @Override
    public Boolean asBoolean() {
        if (null == data) {
            return null;
        }
        return (Boolean) data;
    }

    @Override
    public byte[] asBytes() {
        throw new RuntimeException(String.format("Boolean[\"%s\"]can not cast to byte[].", this.asString()));
    }

    @Override
    public String asString() {
        if (null == data) {
            return null;
        }
        return (Boolean) data ? "true" : "false";
    }

    @Override
    public BigDecimal asBigDecimal() {
        if (null == data) {
            return null;
        }
        return BigDecimal.valueOf((Boolean) data ? 1L : 0L);
    }

    @Override
    public Timestamp asTimestamp() {
        throw new RuntimeException(String.format("Boolean[\"%s\"]can not cast to Timestamp.", this.asString()));
    }
}
