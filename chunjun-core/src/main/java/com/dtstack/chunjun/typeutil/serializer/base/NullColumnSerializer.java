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

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class NullColumnSerializer extends TypeSerializerSingleton<AbstractBaseColumn> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the NullColumnSerializer. */
    public static final NullColumnSerializer INSTANCE = new NullColumnSerializer();

    private static final NullColumn EMPTY = new NullColumn();

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
        target.writeByte(0);
    }

    @Override
    public AbstractBaseColumn deserialize(DataInputView source) throws IOException {
        source.readByte();
        return EMPTY;
    }

    @Override
    public AbstractBaseColumn deserialize(AbstractBaseColumn reuse, DataInputView source)
            throws IOException {
        source.readByte();
        return EMPTY;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeByte(0);
    }

    @Override
    public TypeSerializerSnapshot<AbstractBaseColumn> snapshotConfiguration() {
        return new NullColumnSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class NullColumnSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<AbstractBaseColumn> {

        public NullColumnSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
