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

package com.dtstack.chunjun.typeutil.serializer.base;

import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.column.ArrayColumn;
import com.dtstack.chunjun.element.column.NullColumn;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.typeutil.serializer.AbstractColumnSerializerUtil;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.Arrays;

public class ArrayColumnSerializer extends TypeSerializer<AbstractBaseColumn> {

    private static final long serialVersionUID = 1L;

    private final LogicalType elementType;

    private final TypeSerializerSingleton serializer;
    private final ArrayColumn.ElementGetter elementGetter;

    private static final NullColumn REUSE_NULL = new NullColumn();
    private static final ArrayColumn ARRAY_COLUMN = ArrayColumn.from(new Object[0], 0, false);

    public ArrayColumnSerializer(LogicalType elementType) {
        this.elementType = elementType;
        this.serializer = AbstractColumnSerializerUtil.getBaseSerializer(elementType);
        this.elementGetter = ArrayColumn.createElementGetter(elementType);
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<AbstractBaseColumn> duplicate() {
        return new ArrayColumnSerializer(elementType);
    }

    @Override
    public AbstractBaseColumn createInstance() {
        return ARRAY_COLUMN;
    }

    @Override
    public AbstractBaseColumn copy(AbstractBaseColumn from) {
        if (from == null || from instanceof NullColumn) {
            return REUSE_NULL;
        } else {
            ArrayColumn array = (ArrayColumn) from;
            if (array.isPrimitive()) {
                switch (elementType.getTypeRoot()) {
                    case BOOLEAN:
                        return ArrayColumn.from(
                                Arrays.copyOf(array.asBooleanArray(), array.size()),
                                array.size(),
                                true);
                    case TINYINT:
                        return ArrayColumn.from(
                                Arrays.copyOf(array.asByteArray(), array.size()),
                                array.size(),
                                true);
                    case SMALLINT:
                        return ArrayColumn.from(
                                Arrays.copyOf(array.asShortArray(), array.size()),
                                array.size(),
                                true);
                    case INTEGER:
                    case INTERVAL_YEAR_MONTH:
                        return ArrayColumn.from(
                                Arrays.copyOf(array.asIntArray(), array.size()),
                                array.size(),
                                true);
                    case BIGINT:
                    case INTERVAL_DAY_TIME:
                        return ArrayColumn.from(
                                Arrays.copyOf(array.asLongArray(), array.size()),
                                array.size(),
                                true);
                    case FLOAT:
                        return ArrayColumn.from(
                                Arrays.copyOf(array.asFloatArray(), array.size()),
                                array.size(),
                                true);
                    case DOUBLE:
                        return ArrayColumn.from(
                                Arrays.copyOf(array.asDoubleArray(), array.size()),
                                array.size(),
                                true);
                    default:
                        throw new ChunJunRuntimeException("Unknown primitive type: " + elementType);
                }
            } else {
                Object[] newArray = new Object[array.size()];
                for (int i = 0; i < array.size(); i++) {
                    newArray[i] = serializer.copy(elementGetter.getElementOrNull(array, i));
                }
                return ArrayColumn.from(newArray, array.size(), false);
            }
        }
    }

    @Override
    public AbstractBaseColumn copy(AbstractBaseColumn from, AbstractBaseColumn reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(AbstractBaseColumn record, DataOutputView target) throws IOException {
        if (record == null || record instanceof NullColumn) {
            target.writeBoolean(false);
        } else {
            target.writeBoolean(true);
            ArrayColumn arrayColumn = (ArrayColumn) record;
            target.writeInt(arrayColumn.size());
            for (int i = 0; i < arrayColumn.size(); i++) {
                Object elementOrNull = elementGetter.getElementOrNull(arrayColumn, i);
                if (elementOrNull == null) {
                    target.writeBoolean(false);
                } else {
                    target.writeBoolean(true);
                    serializer.serialize(elementOrNull, target);
                }
            }
        }
    }

    @Override
    public AbstractBaseColumn deserialize(DataInputView source) throws IOException {
        boolean isNotNull = source.readBoolean();
        if (isNotNull) {
            int length = source.readInt();
            Object[] objects = new Object[length];
            for (int i = 0; i < length; i++) {
                boolean isElementNotNull = source.readBoolean();
                if (!isElementNotNull) {
                    objects[i] = null;
                } else {
                    objects[i] = serializer.deserialize(source);
                }
            }
            return ArrayColumn.from(objects, length, false);
        } else {
            return REUSE_NULL;
        }
    }

    @Override
    public AbstractBaseColumn deserialize(AbstractBaseColumn reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        boolean isNotNull = source.readBoolean();
        target.writeBoolean(isNotNull);
        if (isNotNull) {
            int length = source.readInt();
            target.writeInt(length);
            for (int i = 0; i < length; i++) {
                boolean isElementNotNull = source.readBoolean();
                target.writeBoolean(isElementNotNull);
                if (isElementNotNull) {
                    serializer.copy(source, target);
                }
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ArrayColumnSerializer
                && this.elementType.equals(((ArrayColumnSerializer) obj).elementType);
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public TypeSerializerSnapshot<AbstractBaseColumn> snapshotConfiguration() {
        return new ArrayColumnSerializerSnapshot(elementType);
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class ArrayColumnSerializerSnapshot
            implements TypeSerializerSnapshot<AbstractBaseColumn> {

        private static final int CURRENT_VERSION = 3;
        private static final StringSerializer stringSerializer = StringSerializer.INSTANCE;
        private LogicalType elementType;

        @SuppressWarnings("unused")
        public ArrayColumnSerializerSnapshot() {
            // this constructor is used when restoring from a checkpoint/savepoint.
        }

        public ArrayColumnSerializerSnapshot(LogicalType elementType) {
            this.elementType = elementType;
        }

        @Override
        public int getCurrentVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            DataOutputViewStream outStream = new DataOutputViewStream(out);
            InstantiationUtil.serializeObject(outStream, elementType);
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            DataInputViewStream inStream = new DataInputViewStream(in);
            try {
                this.elementType =
                        InstantiationUtil.deserializeObject(inStream, userCodeClassLoader);
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
        }

        @Override
        public TypeSerializer<AbstractBaseColumn> restoreSerializer() {
            return new ArrayColumnSerializer(elementType);
        }

        @Override
        public TypeSerializerSchemaCompatibility<AbstractBaseColumn> resolveSchemaCompatibility(
                TypeSerializer<AbstractBaseColumn> newSerializer) {
            if (!(newSerializer instanceof ArrayColumnSerializer)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            ArrayColumnSerializer that = (ArrayColumnSerializer) newSerializer;
            if (!this.elementType.equals(that.elementType)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            } else {
                return TypeSerializerSchemaCompatibility.compatibleAsIs();
            }
        }
    }
}
