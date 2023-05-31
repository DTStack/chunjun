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
import com.dtstack.chunjun.element.column.ZonedTimestampColumn;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class ZonedTimestampColumnSerializer extends TypeSerializerSingleton<AbstractBaseColumn> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the ZonedTimestampColumnSerializer. */
    public static final ZonedTimestampColumnSerializer INSTANCE =
            new ZonedTimestampColumnSerializer();

    private static final NullColumn REUSE_NULL = new NullColumn();
    private static final ZonedTimestampColumn EMPTY = new ZonedTimestampColumn(0);

    @Override
    public boolean isImmutableType() {
        return false;
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
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(AbstractBaseColumn record, DataOutputView target) throws IOException {
        if (record == null || record instanceof NullColumn) {
            target.writeLong(Long.MIN_VALUE);
        } else {
            ZonedTimestampColumn timestampZoneColumn = (ZonedTimestampColumn) record;
            target.writeLong((Long) timestampZoneColumn.getData());
            int precision = timestampZoneColumn.getPrecision();
            target.writeByte(precision);
            if (precision > 3) {
                target.writeInt(timestampZoneColumn.getNanos());
            }
            target.writeInt(timestampZoneColumn.getMillisecondOffset());
        }
    }

    @Override
    public AbstractBaseColumn deserialize(DataInputView source) throws IOException {
        long value = source.readLong();
        if (value == Long.MIN_VALUE) {
            return REUSE_NULL;
        } else {
            int precision = source.readByte();
            if (precision > 3) {
                return ZonedTimestampColumn.from(
                        value, source.readInt(), source.readInt(), precision);
            } else {
                return ZonedTimestampColumn.from(value, 0, source.readInt(), precision);
            }
        }
    }

    @Override
    public AbstractBaseColumn deserialize(AbstractBaseColumn reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        long millisecond = source.readLong();
        target.writeLong(millisecond);
        if (Long.MIN_VALUE != millisecond) {
            int precision = source.readByte();
            target.writeByte(precision);
            if (precision > 3) {
                // nano
                target.writeInt(source.readInt());
            }
            // offset
            target.writeInt(source.readInt());
        }
    }

    @Override
    public TypeSerializerSnapshot<AbstractBaseColumn> snapshotConfiguration() {
        return new ZonedTimestampColumnSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class ZonedTimestampColumnSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<AbstractBaseColumn> {

        public ZonedTimestampColumnSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
