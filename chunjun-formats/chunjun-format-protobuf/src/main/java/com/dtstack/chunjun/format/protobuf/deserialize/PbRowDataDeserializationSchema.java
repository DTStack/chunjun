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

package com.dtstack.chunjun.format.protobuf.deserialize;

import com.dtstack.chunjun.format.protobuf.util.FormatCheckUtil;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.Objects;

/** @author liuliu 2022/4/8 */
public class PbRowDataDeserializationSchema implements DeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private PbParser protoMessageTransformer;
    private PbToRowDataPbConverter.ProtoToRowDataConverter runtimeConverter;
    private TypeInformation<RowData> typeInformation;
    private String messageClassName;
    private RowType rowType;

    public PbRowDataDeserializationSchema(
            RowType rowType, TypeInformation<RowData> typeInformation, String protoOutClassName) {
        this.rowType = rowType;
        this.typeInformation = typeInformation;
        this.messageClassName = protoOutClassName;
        new FormatCheckUtil(rowType, messageClassName).isValid();
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        protoMessageTransformer = new PbParser(messageClassName);
        runtimeConverter = PbToRowDataPbConverter.initMessageDataConverter(messageClassName);
    }

    @Override
    public RowData deserialize(byte[] message) {
        if (message == null) {
            return null;
        }
        Object protoMessage = protoMessageTransformer.parse(message);
        try {
            return (RowData) runtimeConverter.convert(protoMessage);
        } catch (Exception e) {
            throw new ChunJunRuntimeException("Failed to convert protobuf record to RowData", e);
        }
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return typeInformation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PbRowDataDeserializationSchema that = (PbRowDataDeserializationSchema) o;
        return messageClassName.equals(that.messageClassName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageClassName);
    }
}
