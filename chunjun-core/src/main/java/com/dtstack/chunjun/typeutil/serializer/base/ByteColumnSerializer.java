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
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.ByteColumn;
import com.dtstack.chunjun.element.column.NullColumn;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

/** @author liuliu 2022/5/12 */
public class ByteColumnSerializer extends TypeSerializerSingleton<AbstractBaseColumn> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the ByteColumnSerializer. */
    public static final ByteColumnSerializer INSTANCE = new ByteColumnSerializer();

    private static final ByteColumn EMPTY = new ByteColumn((byte) 0);

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
        } else if (record instanceof BigDecimalColumn) {
            target.writeInt(2);
            BigDecimal bigDecimal = record.asBigDecimal();
            byte[] bytes = bigDecimal.unscaledValue().toByteArray();
            target.writeInt(bytes.length);
            target.write(bytes);
            target.writeInt(bigDecimal.scale());
        } else {
            target.writeInt(1);
            target.writeByte((byte) record.getData());
        }
    }

    @Override
    public AbstractBaseColumn deserialize(DataInputView source) throws IOException {
        int type = source.readInt();
        switch (type) {
            case 0:
                return new NullColumn();
            case 1:
                return ByteColumn.from(source.readByte());
            case 2:
                int length = source.readInt();
                byte[] bytes = new byte[length];
                source.readFully(bytes);
                int scale = source.readInt();
                return BigDecimalColumn.from(new BigDecimal(new BigInteger(bytes), scale));
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
            target.writeByte(source.readByte());
        } else if (type == 2) {
            int len = source.readInt();
            target.writeInt(len);
            target.write(source, len);
            target.writeInt(source.readInt());
        }
    }

    @Override
    public TypeSerializerSnapshot<AbstractBaseColumn> snapshotConfiguration() {
        return new ByteColumnSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class ByteColumnSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<AbstractBaseColumn> {

        public ByteColumnSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
