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
import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.BytesColumn;
import com.dtstack.chunjun.element.column.NullColumn;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/** @author liuliu 2022/5/12 */
public class BooleanColumnSerializer extends TypeSerializerSingleton<AbstractBaseColumn> {

    /** Sharable instance of the BooleanColumnSerializer. */
    public static final BooleanColumnSerializer INSTANCE = new BooleanColumnSerializer();

    private static final BooleanColumn FALSE = new BooleanColumn(false);

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public AbstractBaseColumn createInstance() {
        return FALSE;
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
            target.writeInt(0);
        } else if (record instanceof BytesColumn) {
            target.writeInt(2);
            byte[] bytes = record.asBytes();
            target.writeInt(bytes.length);
            target.write(bytes);
        } else {
            target.writeInt(1);
            target.writeBoolean(record.asBoolean());
        }
    }

    @Override
    public AbstractBaseColumn deserialize(DataInputView source) throws IOException {
        int value = source.readInt();
        switch (value) {
            case 0:
                return new NullColumn();
            case 1:
                return BooleanColumn.from(source.readBoolean());
            case 2:
                int len = source.readInt();
                byte[] result = new byte[len];
                source.readFully(result);
                return BytesColumn.from(result);
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
        if (type == 1) {
            target.writeBoolean(source.readBoolean());
        } else if (type == 2) {
            int len = source.readInt();
            byte[] result = new byte[len];
            source.readFully(result);
            target.writeInt(len);
            target.write(result);
        }
    }

    @Override
    public TypeSerializerSnapshot<AbstractBaseColumn> snapshotConfiguration() {
        return new BooleanColumnSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class BooleanColumnSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<AbstractBaseColumn> {

        public BooleanColumnSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
