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

import com.dtstack.chunjun.format.protobuf.PbFormatType;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.binary.BinaryMapData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ProtocolMessageEnum;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.dtstack.chunjun.format.protobuf.util.PbReflectUtil.getClassByClassName;
import static com.dtstack.chunjun.format.protobuf.util.PbReflectUtil.getClassByFieldDescriptor;
import static com.dtstack.chunjun.format.protobuf.util.PbReflectUtil.getClassNameByFullName;
import static com.dtstack.chunjun.format.protobuf.util.PbReflectUtil.getDescriptorByMessageClassName;
import static com.dtstack.chunjun.format.protobuf.util.PbReflectUtil.getMapTypeTuple;

/** @author liuliu 2022/4/26 */
public class RowDataToPbConverter {

    /** proto class full name example as com.dtstack.messageOutClass */
    protected static String PROTO_CLASS_NAME;
    /** proto class package name example as com.dtstack */
    protected static String PROTO_PACKAGE_NAME;

    public interface RowDataToProtoConverter extends Serializable {
        Object convert(Object object) throws InvocationTargetException, IllegalAccessException;
    }

    /**
     * create a runtime converter for row by specific protoMessageClassName and init {@link
     * RowDataToPbConverter#PROTO_PACKAGE_NAME} and {@link RowDataToPbConverter#PROTO_CLASS_NAME}
     */
    public static RowDataToProtoConverter initMessageDataConverter(
            LogicalType logicalType, String messageClassName) {
        try {
            PROTO_CLASS_NAME = messageClassName.substring(0, messageClassName.lastIndexOf("$"));
            PROTO_PACKAGE_NAME = PROTO_CLASS_NAME.substring(0, PROTO_CLASS_NAME.lastIndexOf("."));
            return createMessageDataConverter(logicalType, messageClassName);
        } catch (IndexOutOfBoundsException e) {
            throw new ChunJunRuntimeException(
                    String.format("Incorrect proto message class name:%s", messageClassName), e);
        }
    }

    /**
     * create a runtime converter for row by specific messageClassName example as
     * com.dtstack.MessageOutClass$Message
     */
    private static RowDataToProtoConverter createMessageDataConverter(
            LogicalType logicalType, String protoMessageClassName) {
        Tuple2<Descriptors.Descriptor, Class<? extends AbstractMessage>>
                descriptorByMessageClassName =
                        getDescriptorByMessageClassName(protoMessageClassName);
        return createMessageDataConverter(
                logicalType,
                descriptorByMessageClassName.f0.getFields(),
                descriptorByMessageClassName.f0.getOneofs(),
                descriptorByMessageClassName.f1);
    }

    /** create a runtime converter for row by specific fieldDescriptor */
    public static RowDataToProtoConverter createMessageDataConverter(
            LogicalType logicalType, Descriptors.FieldDescriptor fieldDescriptor) {

        Descriptors.Descriptor messageType = fieldDescriptor.getMessageType();
        return createMessageDataConverter(
                logicalType,
                messageType.getFields(),
                messageType.getOneofs(),
                getClassByFieldDescriptor(
                        PROTO_CLASS_NAME, PROTO_PACKAGE_NAME, fieldDescriptor, null));
    }

    private static RowDataToProtoConverter createMessageDataConverter(
            LogicalType logicalType,
            List<Descriptors.FieldDescriptor> fieldDescriptorList,
            List<Descriptors.OneofDescriptor> oneofDescriptorList,
            Class<? extends AbstractMessage> clazz) {
        RowDataToProtoConverter[] rowDataToProtoConverters =
                new RowDataToProtoConverter[fieldDescriptorList.size()];
        PbMessageSetter pbMessageSetter =
                new PbMessageSetter(fieldDescriptorList, oneofDescriptorList, clazz);
        int currentIndex = 0;
        for (LogicalType childLogicalType : logicalType.getChildren()) {
            if (childLogicalType instanceof RowType && pbMessageSetter.isOneOf(currentIndex)) {
                for (int i = 1; i < ((RowType) childLogicalType).getFieldCount(); i++) {
                    rowDataToProtoConverters[currentIndex] =
                            createNullableConverter(
                                    ((RowType) childLogicalType).getTypeAt(i),
                                    fieldDescriptorList.get(currentIndex++));
                }
            } else {
                rowDataToProtoConverters[currentIndex] =
                        createNullableConverter(
                                childLogicalType, fieldDescriptorList.get(currentIndex++));
            }
        }
        return object -> {
            GenericRowData genericRowData = (GenericRowData) object;
            for (int i = 0, index = 0;
                    i < fieldDescriptorList.size() && index < genericRowData.getArity();
                    i++) {
                if (pbMessageSetter.isOneOf(i)) {
                    GenericRowData oneofGenericRowData =
                            (GenericRowData) genericRowData.getField(index++);
                    int oneofCase = oneofGenericRowData.getInt(0);
                    int caseInRow = oneofCase - i;
                    pbMessageSetter.setByIndex(
                            rowDataToProtoConverters[oneofCase - 1].convert(
                                    oneofGenericRowData.getField(caseInRow)),
                            oneofCase - 1);
                    i = pbMessageSetter.getLastOneOfIndex(i);
                } else {
                    pbMessageSetter.setByIndex(
                            rowDataToProtoConverters[i].convert(genericRowData.getField(index++)),
                            i);
                }
            }
            return pbMessageSetter.build();
        };
    }

    /** Creates a runtime converter which is null safe. */
    private static RowDataToProtoConverter createNullableConverter(
            LogicalType logicalType, Descriptors.FieldDescriptor descriptor) {
        return createNullableConverter(
                logicalType, descriptor, PbFormatType.getTypeByFieldDescriptor(descriptor));
    }

    private static RowDataToProtoConverter createNullableConverter(
            LogicalType logicalType,
            Descriptors.FieldDescriptor descriptor,
            PbFormatType protoType) {
        final RowDataToProtoConverter converter =
                createConverter(logicalType, descriptor, protoType);
        return protoObject -> {
            if (protoObject == null) {
                return null;
            }
            return converter.convert(protoObject);
        };
    }

    public static RowDataToProtoConverter createConverter(
            LogicalType logicalType,
            Descriptors.FieldDescriptor fieldDescriptor,
            PbFormatType protoType) {
        switch (protoType) {
            case ENUM:
                return createEnumConverter(fieldDescriptor);
            case STRING:
                return Object::toString;
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
                return object -> object;
            case BYTE_STRING:
                return object -> ByteString.copyFrom((byte[]) object);
            case MAP:
                return createMapConverter(logicalType, fieldDescriptor);
            case MESSAGE:
                return createMessageDataConverter(logicalType, fieldDescriptor);
            case ARRAY:
                return createArrayConverter(logicalType, fieldDescriptor);
            default:
                throw new UnsupportedOperationException(protoType.name());
        }
    }

    /**
     * In standard ProtocolBuffers, mapKey can be any integral or string type (so, any scalar type
     * except for floating point types and bytes), mapValue not can be mapType and arrayType
     *
     * @return a runtime map converter
     */
    private static RowDataToProtoConverter createMapConverter(
            LogicalType logicalType, Descriptors.FieldDescriptor fieldDescriptor) {
        Tuple4<PbFormatType, PbFormatType, Class, Class> mapTypeTuple =
                getMapTypeTuple(PROTO_CLASS_NAME, PROTO_PACKAGE_NAME, fieldDescriptor);
        LogicalType keyType = ((MapType) logicalType).getKeyType();
        LogicalType valueType = ((MapType) logicalType).getValueType();
        // create keyConverter and valueConverter
        final RowDataToProtoConverter keyConverter =
                createNullableConverter(keyType, null, mapTypeTuple.f0);
        final RowDataToProtoConverter valueConverter;
        if (mapTypeTuple.f1 == PbFormatType.MESSAGE) {
            valueConverter = createMessageDataConverter(valueType, mapTypeTuple.f3.getName());
        } else {
            // scalar type/enum
            valueConverter = createConverter(valueType, null, mapTypeTuple.f1);
        }

        return object -> {
            Map<Object, Object> result = new HashMap<>();
            Object[] keyArray;
            Object[] valueArray;
            if (object instanceof BinaryMapData) {
                keyArray = ((BinaryMapData) object).keyArray().toObjectArray(keyType);
                valueArray = ((BinaryMapData) object).valueArray().toObjectArray(valueType);
            } else if (object instanceof GenericMapData) {
                keyArray =
                        ((GenericArrayData) ((GenericMapData) object).keyArray()).toObjectArray();
                valueArray =
                        ((GenericArrayData) ((GenericMapData) object).valueArray()).toObjectArray();
            } else {
                throw new UnsupportedOperationException(
                        "protobuf-x format Map serializer only support BinaryMapData and GenericMapData");
            }
            for (int i = 0; i < keyArray.length; i++) {
                result.put(
                        keyConverter.convert(keyArray[i]), valueConverter.convert(valueArray[i]));
            }
            return result;
        };
    }

    /**
     * In standard ProtocolBuffers, element can only be map、message、scalar
     *
     * @return a runtime array converter
     */
    private static RowDataToProtoConverter createArrayConverter(
            LogicalType logicalType, Descriptors.FieldDescriptor fieldDescriptor) {
        PbFormatType protoType = PbFormatType.getArrayInnerTypeByFieldDescriptor(fieldDescriptor);
        LogicalType elementType = ((ArrayType) logicalType).getElementType();
        final RowDataToProtoConverter elementConverter =
                createConverter(elementType, fieldDescriptor, protoType);
        return object -> {
            List<Object> list = new ArrayList<>();
            Object[] elements;
            if (object instanceof BinaryArrayData) {
                elements = ((BinaryArrayData) object).toObjectArray(elementType);
            } else if (object instanceof GenericArrayData) {
                elements = ((GenericArrayData) object).toObjectArray();
            } else {
                throw new UnsupportedOperationException(
                        "protobuf-x format Repeated serializer only support BinaryArrayData and GenericArrayData");
            }
            for (Object o : elements) {
                list.add(elementConverter.convert(o));
            }
            return list;
        };
    }

    private static RowDataToProtoConverter createEnumConverter(
            Descriptors.FieldDescriptor fieldDescriptor) {
        EnumMessageAssemblers assemblers = new EnumMessageAssemblers(fieldDescriptor);
        return object -> assemblers.valueOf(object.toString());
    }

    private static class EnumMessageAssemblers {

        private final Method valueOfMethod;
        private final String enumClassName;
        private final Class<? extends ProtocolMessageEnum> enumClass;

        public EnumMessageAssemblers(Descriptors.FieldDescriptor fieldDescriptor) {
            this.enumClassName =
                    getClassNameByFullName(
                            PROTO_CLASS_NAME,
                            PROTO_PACKAGE_NAME,
                            fieldDescriptor.getEnumType().getFullName());
            enumClass = getClassByClassName(enumClassName);
            try {
                this.valueOfMethod = enumClass.getDeclaredMethod("valueOf", String.class);
                valueOfMethod.setAccessible(true);
            } catch (NoSuchMethodException e) {
                throw new ChunJunRuntimeException(
                        String.format(
                                "Failed to get valueOf method from enum class [%s]",
                                enumClassName));
            }
        }

        public Object valueOf(String enumString) {
            try {
                return valueOfMethod.invoke(enumClass, enumString);
            } catch (Exception e) {
                throw new ChunJunRuntimeException(
                        String.format(
                                "Failed to generate an enum object of class [%s] from the string [%s]",
                                enumClassName, enumString),
                        e);
            }
        }
    }
}
