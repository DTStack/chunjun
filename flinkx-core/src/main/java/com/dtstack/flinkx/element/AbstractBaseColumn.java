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
package com.dtstack.flinkx.element;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

/**
 * Date: 2021/04/26
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public abstract class AbstractBaseColumn implements Serializable {
    private static final long serialVersionUID = 1L;
    protected Object data;
    protected int byteSize;

    public AbstractBaseColumn(final Object data) {
        this.data = data;
        this.byteSize = getByteSize(data);
    }

    public abstract int getByteSize(Object data);

    public abstract String asString();

    public abstract Date asDate();

    public abstract byte[] asBytes();

    public abstract Boolean asBoolean();

    public abstract BigDecimal asBigDecimal();

    public abstract Timestamp asTimestamp();

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public int getByteSize() {
        return byteSize;
    }

    public void setByteSize(int byteSize) {
        this.byteSize = byteSize;
    }

    @Override
    public String toString() {
        return "AbstractBaseColumn{" +
                "data=" + data +
                ", byteSize=" + byteSize +
                '}';
    }
}
