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
import com.dtstack.chunjun.element.column.SqlDateColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;


public class SqlDateColumnSerializer extends TypeSerializerSingleton<AbstractBaseColumn> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the SqlDateColumnSerializer. */
    public static final SqlDateColumnSerializer INSTANCE = new SqlDateColumnSerializer();

    private static final SqlDateColumn EMPTY = new SqlDateColumn(0);

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
        if (from instanceof NullColumn) {
            return new NullColumn();
        }
        if (from instanceof TimestampColumn) {
            return new TimestampColumn(
                    ((Timestamp) from.getData()).getTime(),
                    ((TimestampColumn) from).getPrecision());
        }
        return SqlDateColumn.from(from.asSqlDate());
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
            target.writeInt(0);
        } else if (record instanceof TimestampColumn) {
            target.writeInt(2);
            target.writeLong(record.asTimestamp().getTime());
        } else {
            target.writeInt(1);
            target.writeLong(record.asSqlDate().getTime());
        }
    }

    @Override
    public AbstractBaseColumn deserialize(DataInputView source) throws IOException {
        int type = source.readInt();
        switch (type) {
            case 0:
                return new NullColumn();
            case 1:
                return SqlDateColumn.from(new Date(source.readLong()));
            case 2:
                return TimestampColumn.from(source.readLong(), 0);
            default:
                // you should not be here
                throw new ChunJunRuntimeException("");
        }
    }

    @Override
    public AbstractBaseColumn deserialize(AbstractBaseColumn reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int type = source.readInt();
        target.writeInt(type);
        if (type != 0) {
            target.writeLong(source.readLong());
        }
    }

    @Override
    public TypeSerializerSnapshot<AbstractBaseColumn> snapshotConfiguration() {
        return new SqlDateColumnSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class SqlDateColumnSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<AbstractBaseColumn> {

        public SqlDateColumnSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
