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
import com.dtstack.chunjun.element.column.NullColumn;
import com.dtstack.chunjun.element.column.StringColumn;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;

public class StringColumnSerializer extends TypeSerializer<AbstractBaseColumn> {

    private static final long serialVersionUID = 1L;

    private final StringSerializer stringSerializer = StringSerializer.INSTANCE;

    private final String format;
    private final boolean isCustomFormat;

    private static final NullColumn REUSE_NULL = new NullColumn();
    private static final StringColumn EMPTY = StringColumn.from("", "", false);

    public StringColumnSerializer(String format) {
        if (StringUtils.isNotBlank(format)) {
            this.format = format;
            isCustomFormat = true;
        } else {
            this.format = "yyyy-MM-dd HH:mm:ss";
            isCustomFormat = false;
        }
    }

    public StringColumnSerializer(String format, boolean isCustomFormat) {
        this.format = format;
        this.isCustomFormat = isCustomFormat;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<AbstractBaseColumn> duplicate() {
        return new StringColumnSerializer(format, isCustomFormat);
    }

    @Override
    public AbstractBaseColumn createInstance() {
        return EMPTY;
    }

    @Override
    public AbstractBaseColumn copy(AbstractBaseColumn from) {
        return from;
    }

    @Override
    public AbstractBaseColumn copy(AbstractBaseColumn from, AbstractBaseColumn reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(AbstractBaseColumn record, DataOutputView target) throws IOException {
        if (record == null || record instanceof NullColumn) {
            target.write(0);
        } else {
            StringColumn column = (StringColumn) record;
            stringSerializer.serialize((String) column.getData(), target);
        }
    }

    @Override
    public AbstractBaseColumn deserialize(DataInputView source) throws IOException {
        String data = stringSerializer.deserialize(source);
        if (data != null) {
            return StringColumn.from(data, format, isCustomFormat);
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
        stringSerializer.copy(source, target);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof StringColumnSerializer) {
            StringColumnSerializer that = (StringColumnSerializer) obj;
            return that.isCustomFormat == this.isCustomFormat && that.format.equals(this.format);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public TypeSerializerSnapshot<AbstractBaseColumn> snapshotConfiguration() {
        return new StringColumnSerializerSnapshot(format, isCustomFormat);
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class StringColumnSerializerSnapshot
            implements TypeSerializerSnapshot<AbstractBaseColumn> {

        private static final int CURRENT_VERSION = 3;
        private static final StringSerializer stringSerializer = StringSerializer.INSTANCE;
        private String format;
        private boolean isCustomFormat;

        @SuppressWarnings("unused")
        public StringColumnSerializerSnapshot() {
            // this constructor is used when restoring from a checkpoint/savepoint.
        }

        public StringColumnSerializerSnapshot(String format, boolean isCustomFormat) {
            this.format = format;
            this.isCustomFormat = isCustomFormat;
        }

        @Override
        public int getCurrentVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            stringSerializer.serialize(format, out);
            out.writeBoolean(isCustomFormat);
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            this.format = stringSerializer.deserialize(in);
            this.isCustomFormat = in.readBoolean();
        }

        @Override
        public TypeSerializer<AbstractBaseColumn> restoreSerializer() {
            return new StringColumnSerializer(format, isCustomFormat);
        }

        @Override
        public TypeSerializerSchemaCompatibility<AbstractBaseColumn> resolveSchemaCompatibility(
                TypeSerializer<AbstractBaseColumn> newSerializer) {
            if (!(newSerializer instanceof StringColumnSerializer)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            StringColumnSerializer that = (StringColumnSerializer) newSerializer;
            if (!format.equals(that.format) || isCustomFormat != that.isCustomFormat) {
                return TypeSerializerSchemaCompatibility.incompatible();
            } else {
                return TypeSerializerSchemaCompatibility.compatibleAsIs();
            }
        }
    }
}
