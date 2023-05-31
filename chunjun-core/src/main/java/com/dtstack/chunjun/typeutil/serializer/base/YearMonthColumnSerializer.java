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
import com.dtstack.chunjun.element.column.YearMonthColumn;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.types.logical.YearMonthIntervalType;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class YearMonthColumnSerializer extends TypeSerializer<AbstractBaseColumn> {

    private static final long serialVersionUID = 1L;

    // todo consider precision
    private final YearMonthIntervalType yearMonthIntervalType;
    private final YearMonthColumn.YearMonthFormat yearMonthFormat;

    private static final NullColumn REUSE_NULL = new NullColumn();
    private static final YearMonthColumn EMPTY = YearMonthColumn.from(0);

    public YearMonthColumnSerializer(YearMonthIntervalType yearMonthIntervalType) {
        this.yearMonthIntervalType = yearMonthIntervalType;
        this.yearMonthFormat = YearMonthColumn.getYearMonthFormat(yearMonthIntervalType);
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<AbstractBaseColumn> duplicate() {
        return new YearMonthColumnSerializer(yearMonthIntervalType);
    }

    @Override
    public AbstractBaseColumn createInstance() {
        return EMPTY;
    }

    @Override
    public AbstractBaseColumn copy(AbstractBaseColumn from) {
        if (from == null || from instanceof NullColumn) {
            return REUSE_NULL;
        }
        YearMonthColumn res = YearMonthColumn.from((Integer) from.getData());
        res.setYearMonthFormat(yearMonthFormat);
        return res;
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
            target.writeInt(-1);
        } else {
            target.writeInt((Integer) record.getData());
        }
    }

    @Override
    public AbstractBaseColumn deserialize(DataInputView source) throws IOException {
        int month = source.readInt();
        if (month == -1) {
            return REUSE_NULL;
        } else {
            YearMonthColumn res = YearMonthColumn.from(month);
            res.setYearMonthFormat(yearMonthFormat);
            return res;
        }
    }

    @Override
    public AbstractBaseColumn deserialize(AbstractBaseColumn reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeInt(source.readInt());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof YearMonthColumnSerializer) {
            YearMonthColumnSerializer that = (YearMonthColumnSerializer) obj;
            return that.yearMonthIntervalType.equals(this.yearMonthIntervalType);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public TypeSerializerSnapshot<AbstractBaseColumn> snapshotConfiguration() {
        return new YearMonthColumnSerializerSnapshot(yearMonthIntervalType);
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class YearMonthColumnSerializerSnapshot
            implements TypeSerializerSnapshot<AbstractBaseColumn> {

        private static final int CURRENT_VERSION = 3;

        private YearMonthIntervalType yearMonthIntervalType;

        @SuppressWarnings("unused")
        public YearMonthColumnSerializerSnapshot() {
            // this constructor is used when restoring from a checkpoint/savepoint.
        }

        public YearMonthColumnSerializerSnapshot(YearMonthIntervalType yearMonthIntervalType) {
            this.yearMonthIntervalType = yearMonthIntervalType;
        }

        @Override
        public int getCurrentVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            out.writeByte(yearMonthIntervalType.getYearPrecision());
            switch (yearMonthIntervalType.getResolution()) {
                case YEAR:
                    out.writeByte(1);
                    break;
                case YEAR_TO_MONTH:
                    out.writeByte(2);
                    break;
                case MONTH:
                    out.writeByte(3);
                    break;
                default:
                    throw new UnsupportedEncodingException(
                            yearMonthIntervalType.getResolution().name());
            }
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            int precision = in.readByte();
            int num = in.readByte();
            switch (num) {
                case 1:
                    yearMonthIntervalType =
                            new YearMonthIntervalType(
                                    YearMonthIntervalType.YearMonthResolution.YEAR, precision);
                    break;
                case 2:
                    yearMonthIntervalType =
                            new YearMonthIntervalType(
                                    YearMonthIntervalType.YearMonthResolution.YEAR_TO_MONTH,
                                    precision);
                    break;
                case 3:
                    yearMonthIntervalType =
                            new YearMonthIntervalType(
                                    YearMonthIntervalType.YearMonthResolution.MONTH, precision);
                    break;
                default:
                    throw new ChunJunRuntimeException(
                            String.format(
                                    "illegal YearMonthColumnSerializer snapshot num[%s]", num));
            }
        }

        @Override
        public TypeSerializer<AbstractBaseColumn> restoreSerializer() {
            return new YearMonthColumnSerializer(yearMonthIntervalType);
        }

        @Override
        public TypeSerializerSchemaCompatibility<AbstractBaseColumn> resolveSchemaCompatibility(
                TypeSerializer<AbstractBaseColumn> newSerializer) {
            if (!(newSerializer instanceof YearMonthColumnSerializer)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            YearMonthColumnSerializer that = (YearMonthColumnSerializer) newSerializer;
            if (!this.yearMonthIntervalType.equals(that.yearMonthIntervalType)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            } else {
                return TypeSerializerSchemaCompatibility.compatibleAsIs();
            }
        }
    }
}
