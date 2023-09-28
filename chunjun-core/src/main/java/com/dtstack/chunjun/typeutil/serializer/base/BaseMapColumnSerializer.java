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
import com.dtstack.chunjun.element.column.BaseMapColumn;
import com.dtstack.chunjun.element.column.NullColumn;
import com.dtstack.chunjun.typeutil.serializer.AbstractColumnSerializerUtil;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** @author liuliu 2022/9/5 */
public class BaseMapColumnSerializer extends TypeSerializer<AbstractBaseColumn> {

    private static final long serialVersionUID = 1L;

    private final LogicalType keyType;
    private final LogicalType valueType;

    private final TypeSerializerSingleton keySerializer;
    private final TypeSerializerSingleton valueSerializer;

    private final ArrayColumn.ElementGetter keyGetter;
    private final ArrayColumn.ElementGetter valueGetter;

    private static final NullColumn REUSE_NULL = new NullColumn();
    private static final BaseMapColumn BASE_MAP_COLUMN = BaseMapColumn.from(new HashMap<>());

    public BaseMapColumnSerializer(LogicalType keyType, LogicalType valueType) {
        this.keyType = keyType;
        this.valueType = valueType;
        this.keySerializer = AbstractColumnSerializerUtil.getBaseSerializer(keyType);
        this.valueSerializer = AbstractColumnSerializerUtil.getBaseSerializer(valueType);
        this.keyGetter = ArrayColumn.createElementGetter(keyType);
        this.valueGetter = ArrayColumn.createElementGetter(valueType);
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<AbstractBaseColumn> duplicate() {
        return new BaseMapColumnSerializer(keyType, valueType);
    }

    @Override
    public AbstractBaseColumn createInstance() {
        return BASE_MAP_COLUMN;
    }

    @Override
    public AbstractBaseColumn copy(AbstractBaseColumn from) {
        if (from == null || from instanceof NullColumn) {
            return REUSE_NULL;
        } else {
            BaseMapColumn baseMapColumn = (BaseMapColumn) from;
            ArrayColumn keyColumn = baseMapColumn.keySet(keyType);
            ArrayColumn valueColumn = baseMapColumn.valueSet(valueType);
            int length = keyColumn.size();
            Map<Object, Object> map = new HashMap<>(length);
            for (int i = 0; i < length; i++) {
                Object key = null;
                Object value = null;
                if (!keyColumn.isNullAt(i)) {
                    key = keySerializer.copy(keyGetter.getElementOrNull(keyColumn, i));
                }
                if (!valueColumn.isNullAt(i)) {
                    value = valueSerializer.copy(valueGetter.getElementOrNull(valueColumn, i));
                }
                map.put(key, value);
            }
            return BaseMapColumn.from(map);
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
            BaseMapColumn baseMapColumn = (BaseMapColumn) record;
            ArrayColumn keyColumn = baseMapColumn.keySet(keyType);
            ArrayColumn valueColumn = baseMapColumn.valueSet(valueType);
            target.writeInt(keyColumn.size());
            for (int i = 0; i < keyColumn.size(); i++) {
                Object key = keyGetter.getElementOrNull(keyColumn, i);
                Object value = valueGetter.getElementOrNull(valueColumn, i);
                if (key == null) {
                    target.writeBoolean(false);
                } else {
                    target.writeBoolean(true);
                    keySerializer.serialize(key, target);
                }
                if (value == null) {
                    target.writeBoolean(false);
                } else {
                    target.writeBoolean(true);
                    valueSerializer.serialize(value, target);
                }
            }
        }
    }

    @Override
    public AbstractBaseColumn deserialize(DataInputView source) throws IOException {
        boolean isNotNull = source.readBoolean();
        if (isNotNull) {
            int length = source.readInt();
            Map<Object, Object> map = new HashMap<>(length);
            for (int i = 0; i < length; i++) {

                Object key = null;
                Object value = null;
                boolean isKeyNotNull = source.readBoolean();
                if (isKeyNotNull) {
                    key = keySerializer.deserialize(source);
                }

                boolean isValueNotNull = source.readBoolean();
                if (isValueNotNull) {
                    value = valueSerializer.deserialize(source);
                }
                map.put(key, value);
            }
            return BaseMapColumn.from(map);
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
                boolean isKeyNotNull = source.readBoolean();
                target.writeBoolean(isKeyNotNull);
                if (isKeyNotNull) {
                    keySerializer.copy(source, target);
                }
                boolean isValueNotNull = source.readBoolean();
                target.writeBoolean(isValueNotNull);
                if (isValueNotNull) {
                    valueSerializer.copy(source, target);
                }
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof BaseMapColumnSerializer
                && this.keyType.equals(((BaseMapColumnSerializer) obj).keyType)
                && this.valueType.equals(((BaseMapColumnSerializer) obj).valueType);
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public TypeSerializerSnapshot<AbstractBaseColumn> snapshotConfiguration() {
        return new BaseMapColumnSerializerSnapshot(keyType, valueType);
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class BaseMapColumnSerializerSnapshot
            implements TypeSerializerSnapshot<AbstractBaseColumn> {

        private static final int CURRENT_VERSION = 3;
        private LogicalType keyType;
        private LogicalType valueType;

        @SuppressWarnings("unused")
        public BaseMapColumnSerializerSnapshot() {
            // this constructor is used when restoring from a checkpoint/savepoint.
        }

        public BaseMapColumnSerializerSnapshot(LogicalType keyType, LogicalType valueType) {
            this.keyType = keyType;
            this.valueType = valueType;
        }

        @Override
        public int getCurrentVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            DataOutputViewStream outStream = new DataOutputViewStream(out);
            InstantiationUtil.serializeObject(outStream, keyType);
            InstantiationUtil.serializeObject(outStream, valueType);
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            DataInputViewStream inStream = new DataInputViewStream(in);
            try {
                this.keyType = InstantiationUtil.deserializeObject(inStream, userCodeClassLoader);
                this.valueType = InstantiationUtil.deserializeObject(inStream, userCodeClassLoader);
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
        }

        @Override
        public TypeSerializer<AbstractBaseColumn> restoreSerializer() {
            return new BaseMapColumnSerializer(keyType, valueType);
        }

        @Override
        public TypeSerializerSchemaCompatibility<AbstractBaseColumn> resolveSchemaCompatibility(
                TypeSerializer<AbstractBaseColumn> newSerializer) {
            if (!(newSerializer instanceof BaseMapColumnSerializer)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            BaseMapColumnSerializer that = (BaseMapColumnSerializer) newSerializer;
            if (!this.keyType.equals(that.keyType) || !this.valueType.equals(that.valueType)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            } else {
                return TypeSerializerSchemaCompatibility.compatibleAsIs();
            }
        }
    }
}
