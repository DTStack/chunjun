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

import com.dtstack.chunjun.format.protobuf.PbFormatType;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.dtstack.chunjun.format.protobuf.util.PbReflectUtil.getClassByFieldDescriptor;
import static com.dtstack.chunjun.format.protobuf.util.PbReflectUtil.getDescriptorByMessageClassName;
import static com.dtstack.chunjun.format.protobuf.util.PbReflectUtil.getMapTypeTuple;

/** @author liuliu 2022/4/11 Converte from proto to RowData */
public class PbToRowDataPbConverter {

    /** proto class full name example as com.dtstack.messageOutClass */
    protected static String PROTO_CLASS_NAME;
    /** proto class package name example as com.dtstack */
    protected static String PROTO_PACKAGE_NAME;

    public interface ProtoToRowDataConverter extends Serializable {
        Object convert(Object object) throws InvocationTargetException, IllegalAccessException;
    }

    /**
     * create a runtime converter for row by specific protoMessageClassName and init {@link
     * PbToRowDataPbConverter#PROTO_PACKAGE_NAME} and {@link
     * PbToRowDataPbConverter#PROTO_CLASS_NAME}
     */
    public static ProtoToRowDataConverter initMessageDataConverter(String messageClassName) {
        try {
            PROTO_CLASS_NAME = messageClassName.substring(0, messageClassName.lastIndexOf("$"));
            PROTO_PACKAGE_NAME = PROTO_CLASS_NAME.substring(0, PROTO_CLASS_NAME.lastIndexOf("."));
            return createMessageDataConverter(messageClassName);
        } catch (IndexOutOfBoundsException e) {
            throw new ChunJunRuntimeException(
                    String.format("Incorrect proto message class name:%s", messageClassName), e);
        }
    }

    /**
     * create a runtime converter for row by specific messageClassName example as
     * com.dtstack.MessageOutClass$Message
     */
    public static ProtoToRowDataConverter createMessageDataConverter(String protoMessageClassName) {
        Tuple2<Descriptors.Descriptor, Class<? extends AbstractMessage>>
                descriptorByMessageClassName =
                        getDescriptorByMessageClassName(protoMessageClassName);

        return createMessageDataConverter(
                descriptorByMessageClassName.f0.getFields(),
                descriptorByMessageClassName.f0.getOneofs(),
                descriptorByMessageClassName.f1);
    }

    /** create a runtime converter for row by specific fieldDescriptor */
    public static ProtoToRowDataConverter createMessageDataConverter(
            Descriptors.FieldDescriptor fieldDescriptor) {

        Descriptors.Descriptor messageType = fieldDescriptor.getMessageType();
        return createMessageDataConverter(
                messageType.getFields(),
                messageType.getOneofs(),
                getClassByFieldDescriptor(
                        PROTO_CLASS_NAME, PROTO_PACKAGE_NAME, fieldDescriptor, null));
    }

    public static ProtoToRowDataConverter createMessageDataConverter(
            List<Descriptors.FieldDescriptor> fieldDescriptorList,
            List<Descriptors.OneofDescriptor> oneofDescriptorList,
            Class<? extends AbstractMessage> clazz) {
        PbMessageGetter pbMessageGetter =
                new PbMessageGetter(fieldDescriptorList, oneofDescriptorList, clazz);
        ProtoToRowDataConverter[] protoToRowDataConverters =
                fieldDescriptorList.stream()
                        .map(PbToRowDataPbConverter::createNullableConverter)
                        .toArray(ProtoToRowDataConverter[]::new);
        return object -> {
            int size = pbMessageGetter.getSize();
            GenericRowData genericRowData = new GenericRowData(size);
            for (int i = 0, index = 0; i < protoToRowDataConverters.length && index < size; ) {
                if (pbMessageGetter.isOneOf(i)) {
                    int lastIndex = pbMessageGetter.getLastOneOfIndex(i);
                    GenericRowData oneofGenericRowData = new GenericRowData(lastIndex - i + 2);
                    oneofGenericRowData.setField(0, pbMessageGetter.getOneofCase(object, i));
                    int currentCase = 1;
                    while (currentCase < oneofGenericRowData.getArity()) {
                        Object o = pbMessageGetter.getByIndex(object, i);
                        oneofGenericRowData.setField(
                                currentCase++, protoToRowDataConverters[i++].convert(o));
                    }
                    genericRowData.setField(index++, oneofGenericRowData);
                } else {
                    Object o = pbMessageGetter.getByIndex(object, i);
                    genericRowData.setField(index++, protoToRowDataConverters[i++].convert(o));
                }
            }
            return genericRowData;
        };
    }

    /** Creates a runtime converter which is null safe. */
    private static ProtoToRowDataConverter createNullableConverter(
            Descriptors.FieldDescriptor descriptor) {
        return createNullableConverter(
                descriptor, PbFormatType.getTypeByFieldDescriptor(descriptor));
    }

    private static ProtoToRowDataConverter createNullableConverter(
            Descriptors.FieldDescriptor descriptor, PbFormatType protoType) {
        final ProtoToRowDataConverter converter = createConverter(descriptor, protoType);
        return protoObject -> {
            if (protoObject == null) {
                return null;
            }
            return converter.convert(protoObject);
        };
    }

    /**
     * In standard ProtocolBuffers, mapKey can be any integral or string type (so, any scalar type
     * except for floating point types and bytes), mapValue not can be mapType and arrayType
     *
     * @return a runtime map converter
     */
    public static ProtoToRowDataConverter createMapConverter(
            Descriptors.FieldDescriptor fieldDescriptor) {
        // get builder method
        Tuple4<PbFormatType, PbFormatType, Class, Class> mapTypeTuple =
                getMapTypeTuple(PROTO_CLASS_NAME, PROTO_PACKAGE_NAME, fieldDescriptor);
        // create keyConverter and valueConverter
        final ProtoToRowDataConverter keyConverter = createNullableConverter(null, mapTypeTuple.f0);
        final ProtoToRowDataConverter valueConverter;
        if (mapTypeTuple.f1 == PbFormatType.MESSAGE) {
            valueConverter = createMessageDataConverter(mapTypeTuple.f3.getName());
        } else {
            // scalar type/enum
            valueConverter = createConverter(null, mapTypeTuple.f1);
        }
        // create runtime map converter
        return object -> {
            final Map<?, ?> map = (Map<?, ?>) object;
            Map<Object, Object> result = new HashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                Object key = keyConverter.convert(entry.getKey());
                Object value = valueConverter.convert(entry.getValue());
                result.put(key, value);
            }
            return new GenericMapData(result);
        };
    }

    /**
     * In standard ProtocolBuffers, element can only be map、message、scalar
     *
     * @return a runtime array converter
     */
    public static ProtoToRowDataConverter createArrayConverter(
            Descriptors.FieldDescriptor fieldDescriptor) {
        PbFormatType protoType = PbFormatType.getArrayInnerTypeByFieldDescriptor(fieldDescriptor);
        final ProtoToRowDataConverter elementConverter =
                createConverter(fieldDescriptor, protoType);
        return object -> {
            final List<?> list = (List<?>) object;
            final int length = list.size();
            final Object[] array = new Object[length];
            for (int i = 0; i < length; ++i) {
                array[i] = elementConverter.convert(list.get(i));
            }
            return new GenericArrayData(array);
        };
    }

    public static ProtoToRowDataConverter createConverter(
            Descriptors.FieldDescriptor fieldDescriptor, PbFormatType protoType) {
        switch (protoType) {
            case ENUM:
            case STRING:
                return object -> StringData.fromString(object.toString());
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
                return object -> object;
            case BYTE_STRING:
                return object -> ((ByteString) object).toByteArray();
            case MAP:
                return createMapConverter(fieldDescriptor);
            case MESSAGE:
                return createMessageDataConverter(fieldDescriptor);
            case ARRAY:
                return createArrayConverter(fieldDescriptor);
            default:
                throw new UnsupportedOperationException(protoType.name());
        }
    }
}
