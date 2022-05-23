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

package com.dtstack.chunjun.format.protobuf.util;

import com.dtstack.chunjun.format.protobuf.PbFormatType;
import com.dtstack.chunjun.format.protobuf.PbMessageAdaptor;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.Descriptors;

import java.util.ArrayList;
import java.util.List;

import static com.dtstack.chunjun.format.protobuf.util.PbReflectUtil.getMapTypeTuple;

/** @author liuliu 2022/5/11 */
public class FormatCheckUtil {

    /** proto class full name example as com.dtstack.messageOutClass */
    protected String PROTO_CLASS_NAME;
    /** proto class package name example as com.dtstack */
    protected String PROTO_PACKAGE_NAME;

    protected RowType rowType;
    protected String messageClassName;

    public FormatCheckUtil(RowType rowType, String messageClassName) {
        this.PROTO_CLASS_NAME = messageClassName.substring(0, messageClassName.lastIndexOf("$"));
        this.PROTO_PACKAGE_NAME = PROTO_CLASS_NAME.substring(0, PROTO_CLASS_NAME.lastIndexOf("."));
        this.rowType = rowType;
        this.messageClassName = messageClassName;
    }

    /** Checks whether the DDL statement matches the Protobuf message */
    public void isValid() {
        RowType messageLogicalType = createMessageLogicalType(messageClassName);
        if (!rowTypeEqual(messageLogicalType, rowType)) {
            throw new ChunJunRuntimeException(
                    String.format(
                            "protobuf class %s except rowType like\n" + " %s\n" + "but\n %s",
                            messageClassName, messageLogicalType, rowType));
        }
    }

    private boolean logicalTypeEqual(LogicalType logicalType, LogicalType target) {
        boolean flag;
        if (logicalType instanceof RowType && target instanceof RowType) {
            flag = rowTypeEqual((RowType) logicalType, (RowType) target);
        } else if (logicalType instanceof MapType && target instanceof MapType) {
            flag = mapTypeEqual((MapType) logicalType, (MapType) target);
        } else if (logicalType instanceof ArrayType && target instanceof ArrayType) {
            flag = arrayTypeEqual((ArrayType) logicalType, (ArrayType) target);
        } else {
            flag = logicalType.getClass() == target.getClass();
        }
        return flag;
    }

    private boolean arrayTypeEqual(ArrayType arrayType, ArrayType target) {
        return logicalTypeEqual(arrayType.getElementType(), target.getElementType());
    }

    private boolean mapTypeEqual(MapType mapType, MapType target) {
        return logicalTypeEqual(mapType.getKeyType(), target.getKeyType())
                && logicalTypeEqual(mapType.getValueType(), target.getValueType());
    }

    public boolean rowTypeEqual(RowType rowType, RowType target) {
        List<RowType.RowField> fields = rowType.getFields();
        List<RowType.RowField> targetFields = target.getFields();
        int size = fields.size();
        if (size == targetFields.size()) {
            for (int i = 0; i < size; i++) {
                RowType.RowField field = fields.get(i);
                RowType.RowField targetField = targetFields.get(i);
                if (!logicalTypeEqual(field.getType(), targetField.getType())) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    /** create rowType by protoMessageClass */
    public RowType createMessageLogicalType(String protoMessageClass) {
        Descriptors.Descriptor descriptor =
                PbReflectUtil.getDescriptorByMessageClassName(protoMessageClass).f0;
        return createMessageLogicalType(descriptor);
    }

    private RowType createMessageLogicalType(Descriptors.Descriptor descriptor) {
        return createMessageLogicalType(descriptor.getFields(), descriptor.getOneofs());
    }

    private RowType createMessageLogicalType(Descriptors.FieldDescriptor descriptor) {
        return createMessageLogicalType(
                descriptor.getMessageType().getFields(), descriptor.getMessageType().getOneofs());
    }

    private RowType createMessageLogicalType(
            List<Descriptors.FieldDescriptor> descriptorList,
            List<Descriptors.OneofDescriptor> oneofDescriptorList) {
        PbMessageAdaptor pbMessageAdaptor =
                new PbMessageAdaptor(descriptorList, oneofDescriptorList);

        List<RowType.RowField> rowFieldList = new ArrayList<>();
        int indexOfOneof = 0;
        for (int i = 0; i < descriptorList.size(); ) {
            if (pbMessageAdaptor.isOneOf(i)) {

                List<RowType.RowField> oneofRowFieldList = new ArrayList<>();
                oneofRowFieldList.add(
                        new RowType.RowField("case", DataTypes.INT().getLogicalType()));
                Integer lastOneOfIndex = pbMessageAdaptor.getLastOneOfIndex(i);
                while (i <= lastOneOfIndex) {
                    Descriptors.FieldDescriptor descriptor = descriptorList.get(i++);
                    oneofRowFieldList.add(
                            new RowType.RowField(
                                    descriptor.getName(), createLogicalType(descriptor)));
                }
                rowFieldList.add(
                        new RowType.RowField(
                                oneofDescriptorList.get(indexOfOneof++).getName(),
                                new RowType(oneofRowFieldList)));

            } else {
                Descriptors.FieldDescriptor descriptor = descriptorList.get(i++);
                rowFieldList.add(
                        new RowType.RowField(descriptor.getName(), createLogicalType(descriptor)));
            }
        }
        return new RowType(rowFieldList);
    }

    private LogicalType createLogicalType(Descriptors.FieldDescriptor fieldDescriptor) {
        return createLogicalType(
                fieldDescriptor, PbFormatType.getTypeByFieldDescriptor(fieldDescriptor));
    }

    private LogicalType createLogicalType(PbFormatType protoType) {
        return createLogicalType(null, protoType);
    }

    private LogicalType createLogicalType(
            Descriptors.FieldDescriptor fieldDescriptor, PbFormatType protoType) {
        switch (protoType) {
            case ENUM:
            case STRING:
                return DataTypes.STRING().getLogicalType();
            case INT:
                return DataTypes.INT().getLogicalType();
            case LONG:
                return DataTypes.BIGINT().getLogicalType();
            case FLOAT:
                return DataTypes.FLOAT().getLogicalType();
            case DOUBLE:
                return DataTypes.DOUBLE().getLogicalType();
            case BOOLEAN:
                return DataTypes.BOOLEAN().getLogicalType();
            case BYTE_STRING:
                return DataTypes.BYTES().getLogicalType();
            case MAP:
                return createMapLogicalType(fieldDescriptor);
            case MESSAGE:
                return createMessageLogicalType(fieldDescriptor);
            case ARRAY:
                return createArrayLogicalType(fieldDescriptor);
            default:
                throw new UnsupportedOperationException(protoType.name());
        }
    }

    private LogicalType createArrayLogicalType(Descriptors.FieldDescriptor fieldDescriptor) {
        PbFormatType protoType = PbFormatType.getArrayInnerTypeByFieldDescriptor(fieldDescriptor);
        return new ArrayType(createLogicalType(fieldDescriptor, protoType));
    }

    private LogicalType createMapLogicalType(Descriptors.FieldDescriptor fieldDescriptor) {
        // get builder method
        Tuple4<PbFormatType, PbFormatType, Class, Class> mapTypeTuple =
                getMapTypeTuple(PROTO_CLASS_NAME, PROTO_PACKAGE_NAME, fieldDescriptor);
        LogicalType keyLogicalType = createLogicalType(mapTypeTuple.f0);
        LogicalType valueLogicalType;
        if (mapTypeTuple.f1 == PbFormatType.MESSAGE) {
            valueLogicalType = createMessageLogicalType(mapTypeTuple.f3.getName());
        } else {
            // scalar type/enum
            valueLogicalType = createLogicalType(mapTypeTuple.f1);
        }
        return new MapType(keyLogicalType, valueLogicalType);
    }
}
