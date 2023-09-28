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
import com.dtstack.chunjun.element.column.TimeColumn;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.sql.Time;

public class TimeColumnSerializer extends TypeSerializerSingleton<AbstractBaseColumn> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the TimeColumnSerializer. */
    public static final TimeColumnSerializer INSTANCE = new TimeColumnSerializer();

    private static final NullColumn REUSE_NULL = new NullColumn();
    private static final TimeColumn EMPTY = new TimeColumn(0);

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
        if (from instanceof NullColumn || from == null) {
            return new NullColumn();
        }
        return TimeColumn.from(from.asTime());
    }

    @Override
    public AbstractBaseColumn copy(AbstractBaseColumn from, AbstractBaseColumn reuse) {
        if (from == null || from instanceof NullColumn) {
            return new NullColumn();
        }
        return TimeColumn.from(from.asTime());
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
            target.writeLong(((Time) record.getData()).getTime());
        }
    }

    @Override
    public AbstractBaseColumn deserialize(DataInputView source) throws IOException {
        long value = source.readLong();
        if (value == Long.MIN_VALUE) {
            return REUSE_NULL;
        } else {
            return TimeColumn.from(new Time(value));
        }
    }

    @Override
    public AbstractBaseColumn deserialize(AbstractBaseColumn reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeLong(source.readLong());
    }

    @Override
    public TypeSerializerSnapshot<AbstractBaseColumn> snapshotConfiguration() {
        return new TimeColumnSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class TimeColumnSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<AbstractBaseColumn> {

        public TimeColumnSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
