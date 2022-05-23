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

package com.dtstack.chunjun.format.protobuf.serialize;

import com.dtstack.chunjun.format.protobuf.util.FormatCheckUtil;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.AbstractMessage;

import java.util.Objects;

/** @author liuliu 2022/4/26 */
public class PbRowDataSerializationSchema implements SerializationSchema<RowData> {
    private static final long serialVersionUID = 1L;

    private final RowType rowType;
    private final String messageClassName;
    private RowDataToPbConverter.RowDataToProtoConverter runtimeConverter;

    public PbRowDataSerializationSchema(RowType rowType, String messageClassName) {
        this.rowType = rowType;
        this.messageClassName = messageClassName;
        new FormatCheckUtil(rowType, messageClassName).isValid();
    }

    @Override
    public void open(InitializationContext context) {
        runtimeConverter = RowDataToPbConverter.initMessageDataConverter(rowType, messageClassName);
    }

    @Override
    public byte[] serialize(RowData element) {
        if (element == null) {
            return null;
        }
        try {
            return ((AbstractMessage) runtimeConverter.convert(element)).toByteArray();
        } catch (Exception e) {
            throw new ChunJunRuntimeException(
                    String.format(
                            "Failed to convert RowData to protobuf record,RowData:%s", element),
                    e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PbRowDataSerializationSchema that = (PbRowDataSerializationSchema) o;
        return messageClassName.equals(that.messageClassName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageClassName);
    }
}
