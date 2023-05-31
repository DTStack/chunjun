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

package com.dtstack.chunjun.typeutil.serializer;

import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.ColumnRowData;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil;
import org.apache.flink.api.common.typeutils.NestedSerializersSnapshotDelegate;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class ColumnRowDataSerializer extends TypeSerializer<RowData> {

    private static final long serialVersionUID = -3193875237503741622L;

    StringSerializer stringSerializer = StringSerializer.INSTANCE;
    int size;

    private final LogicalType[] types;
    private final TypeSerializer<AbstractBaseColumn>[] fieldSerializers;

    @SuppressWarnings("unchecked")
    public ColumnRowDataSerializer(RowType rowType) {
        this(
                rowType.getChildren().toArray(new LogicalType[0]),
                rowType.getFields().stream()
                        .map(
                                field ->
                                        AbstractColumnSerializerUtil.getTypeSerializer(
                                                field.getType(),
                                                field.getDescription().isPresent()
                                                        ? field.getDescription().get()
                                                        : ""))
                        .toArray(TypeSerializer[]::new));
    }

    public ColumnRowDataSerializer(
            LogicalType[] types, TypeSerializer<AbstractBaseColumn>[] fieldSerializers) {
        this.types = types;
        this.fieldSerializers = fieldSerializers;
        this.size = fieldSerializers.length;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<RowData> duplicate() {
        TypeSerializer<AbstractBaseColumn>[] duplicateFieldSerializers =
                new TypeSerializer[fieldSerializers.length];
        for (int i = 0; i < fieldSerializers.length; i++) {
            duplicateFieldSerializers[i] = fieldSerializers[i].duplicate();
        }
        return new ColumnRowDataSerializer(types, duplicateFieldSerializers);
    }

    @Override
    public ColumnRowData createInstance() {
        return new ColumnRowData(fieldSerializers.length);
    }

    @Override
    public RowData copy(RowData from) {
        ColumnRowData that = (ColumnRowData) from;
        ColumnRowData columnRowData = new ColumnRowData(that.getRowKind(), size);
        columnRowData.setHeader(that.getHeaderInfo());
        columnRowData.setExtHeader(that.getExtHeader());
        for (int i = 0; i < size; i++) {
            columnRowData.addField(fieldSerializers[i].copy(that.getField(i)));
        }
        columnRowData.setByteSize(that.getByteSize());
        return columnRowData;
    }

    @Override
    public RowData copy(RowData from, RowData reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    /** rowKind + headerInfoSize + headerInfo + extHeaderSize + extHeader + fields */
    @Override
    public void serialize(RowData record, DataOutputView target) throws IOException {
        ColumnRowData columnRowData = (ColumnRowData) record;
        target.writeByte(record.getRowKind().toByteValue());
        target.writeInt(columnRowData.getByteSize());

        if (columnRowData.getHeaderInfo() == null) {
            target.writeInt(-1);
        } else {
            Map<String, Integer> headerInfo = columnRowData.getHeaderInfo();
            target.writeInt(headerInfo.size());
            for (Map.Entry<String, Integer> entry : headerInfo.entrySet()) {
                stringSerializer.serialize(entry.getKey(), target);
                if (entry.getValue() == null) {
                    target.writeBoolean(false);
                } else {
                    target.writeBoolean(true);
                    target.writeInt(entry.getValue());
                }
            }
        }

        Set<String> extHeader = columnRowData.getExtHeader();
        target.writeInt(extHeader.size());
        for (String ext : columnRowData.getExtHeader()) {
            stringSerializer.serialize(ext, target);
        }

        for (int i = 0; i < size; i++) {
            fieldSerializers[i].serialize(columnRowData.getField(i), target);
        }
    }

    /** rowKind + headerInfoSize + headerInfo + extHeaderSize + extHeader + fields */
    @Override
    public ColumnRowData deserialize(DataInputView source) throws IOException {
        RowKind rowKind = RowKind.fromByteValue(source.readByte());
        int byteSize = source.readInt();
        ColumnRowData columnRowData = new ColumnRowData(rowKind, fieldSerializers.length, byteSize);

        int infoSize = source.readInt();
        if (infoSize >= 0) {
            final LinkedHashMap<String, Integer> headerInfo = new LinkedHashMap<>(infoSize);
            for (int i = 0; i < infoSize; i++) {
                String key = stringSerializer.deserialize(source);
                boolean isNotNull = source.readBoolean();
                Integer value = isNotNull ? source.readInt() : null;
                headerInfo.put(key, value);
            }
            columnRowData.setHeader(headerInfo);
        }

        int extHeaderSize = source.readInt();
        Set<String> extHeader = columnRowData.getExtHeader();
        for (int i = 0; i < extHeaderSize; i++) {
            extHeader.add(stringSerializer.deserialize(source));
        }

        for (TypeSerializer<AbstractBaseColumn> typeSerializer : fieldSerializers) {
            columnRowData.addFieldWithOutByteSize(typeSerializer.deserialize(source));
        }

        return columnRowData;
    }

    @Override
    public ColumnRowData deserialize(RowData reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeByte(source.readByte());
        target.writeInt(source.readInt());
        int infoSize = source.readInt();
        target.writeInt(infoSize);
        for (int i = 0; i < infoSize; i++) {
            stringSerializer.serialize(stringSerializer.deserialize(source), target);
            boolean isNotNull = source.readBoolean();
            target.writeBoolean(isNotNull);
            if (isNotNull) {
                target.writeInt(source.readInt());
            }
        }
        int extHeaderSize = source.readInt();
        target.writeInt(extHeaderSize);
        for (int i = 0; i < extHeaderSize; i++) {
            stringSerializer.serialize(stringSerializer.deserialize(source), target);
        }
        for (TypeSerializer<AbstractBaseColumn> typeSerializer : fieldSerializers) {
            typeSerializer.serialize(typeSerializer.deserialize(source), target);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ColumnRowDataSerializer) {
            ColumnRowDataSerializer other = (ColumnRowDataSerializer) obj;
            return Arrays.equals(fieldSerializers, other.fieldSerializers);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(fieldSerializers);
    }

    @Override
    public TypeSerializerSnapshot<RowData> snapshotConfiguration() {
        return new ColumnRowDataSerializerSnapshot(types, fieldSerializers);
    }

    public static final class ColumnRowDataSerializerSnapshot
            implements TypeSerializerSnapshot<RowData> {
        private static final int CURRENT_VERSION = 3;

        private LogicalType[] previousTypes;
        private NestedSerializersSnapshotDelegate nestedSerializersSnapshotDelegate;

        @SuppressWarnings("unused")
        public ColumnRowDataSerializerSnapshot() {
            // this constructor is used when restoring from a checkpoint/savepoint.
        }

        ColumnRowDataSerializerSnapshot(LogicalType[] types, TypeSerializer[] serializers) {
            this.previousTypes = types;
            this.nestedSerializersSnapshotDelegate =
                    new NestedSerializersSnapshotDelegate(serializers);
        }

        @Override
        public int getCurrentVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            out.writeInt(previousTypes.length);
            DataOutputViewStream stream = new DataOutputViewStream(out);
            for (LogicalType previousType : previousTypes) {
                InstantiationUtil.serializeObject(stream, previousType);
            }
            nestedSerializersSnapshotDelegate.writeNestedSerializerSnapshots(out);
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            int length = in.readInt();
            DataInputViewStream stream = new DataInputViewStream(in);
            previousTypes = new LogicalType[length];
            for (int i = 0; i < length; i++) {
                try {
                    previousTypes[i] =
                            InstantiationUtil.deserializeObject(stream, userCodeClassLoader);
                } catch (ClassNotFoundException e) {
                    throw new IOException(e);
                }
            }
            this.nestedSerializersSnapshotDelegate =
                    NestedSerializersSnapshotDelegate.readNestedSerializerSnapshots(
                            in, userCodeClassLoader);
        }

        @Override
        public ColumnRowDataSerializer restoreSerializer() {
            return new ColumnRowDataSerializer(
                    previousTypes,
                    (TypeSerializer<AbstractBaseColumn>[])
                            nestedSerializersSnapshotDelegate.getRestoredNestedSerializers());
        }

        @Override
        public TypeSerializerSchemaCompatibility<RowData> resolveSchemaCompatibility(
                TypeSerializer<RowData> newSerializer) {
            if (!(newSerializer instanceof ColumnRowDataSerializer)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            ColumnRowDataSerializer newColumnRowSerializer =
                    (ColumnRowDataSerializer) newSerializer;
            if (!Arrays.equals(previousTypes, newColumnRowSerializer.types)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            CompositeTypeSerializerUtil.IntermediateCompatibilityResult<RowData>
                    intermediateResult =
                            CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult(
                                    newColumnRowSerializer.fieldSerializers,
                                    nestedSerializersSnapshotDelegate
                                            .getNestedSerializerSnapshots());

            if (intermediateResult.isCompatibleWithReconfiguredSerializer()) {
                ColumnRowDataSerializer reconfiguredCompositeSerializer = restoreSerializer();
                return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(
                        reconfiguredCompositeSerializer);
            }

            return intermediateResult.getFinalResult();
        }
    }
}
