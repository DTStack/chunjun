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

package com.dtstack.chunjun.format.protobuf;

import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Descriptors;

/** @author liuliu 2022/4/11 */
public enum PbFormatType {
    MAP,
    ENUM,
    ARRAY,
    MESSAGE,
    BOOLEAN,
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    STRING,
    BYTE_STRING;

    PbFormatType() {}

    public static PbFormatType getTypeByFieldDescriptor(
            Descriptors.FieldDescriptor fieldDescriptor) {
        if (fieldDescriptor.isMapField()) {
            return MAP;
        } else if (fieldDescriptor.isRepeated()) {
            return ARRAY;
        } else {
            return getTypeByTypeName(fieldDescriptor.getJavaType().name());
        }
    }

    public static PbFormatType getArrayInnerTypeByFieldDescriptor(
            Descriptors.FieldDescriptor fieldDescriptor) {
        return getTypeByTypeName(fieldDescriptor.getJavaType().name());
    }

    public static PbFormatType getProtoTypeForMapKey(Class clazz) {
        switch (clazz.getName()) {
            case "java.lang.String":
                return STRING;
            case "java.lang.Integer":
            case "int":
                return INT;
            case "java.lang.Long":
            case "long":
                return LONG;
            default:
                throw new ChunJunRuntimeException(
                        String.format(
                                "Map key expect any scalar type except for floating point types and bytes,but it is %s",
                                clazz.getName()));
        }
    }

    public static PbFormatType getProtoTypeForMapValue(Class clazz) {
        if (com.google.protobuf.ProtocolMessageEnum.class.isAssignableFrom(clazz)) {
            return ENUM;
        } else if (AbstractMessage.class.isAssignableFrom(clazz)) {
            return MESSAGE;
        }
        switch (clazz.getName()) {
            case "java.lang.String":
                return STRING;
            case "java.lang.Integer":
            case "int":
                return INT;
            case "java.lang.Double":
            case "double":
                return DOUBLE;
            case "java.lang.Long":
            case "long":
                return LONG;
            case "java.lang.Float":
            case "float":
                return FLOAT;
            case "com.google.protobuf.ByteString":
                return BYTE_STRING;
            default:
                throw new UnsupportedOperationException(clazz.getName());
        }
    }

    public static PbFormatType getTypeByTypeName(String typeName) {
        for (PbFormatType protoType : PbFormatType.values()) {
            if (typeName.equalsIgnoreCase(protoType.name())) {
                return protoType;
            }
        }
        throw new UnsupportedTypeException(typeName);
    }
}
