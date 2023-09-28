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
import com.dtstack.chunjun.element.column.DayTimeColumn;
import com.dtstack.chunjun.element.column.NullColumn;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.types.logical.DayTimeIntervalType;

import java.io.IOException;

public class DayTimeColumnSerializer extends TypeSerializer<AbstractBaseColumn> {

    private static final long serialVersionUID = 1L;

    // todo consider precision
    private final DayTimeIntervalType dayTimeIntervalType;
    private final DayTimeColumn.DayTimeFormat dayTimeFormat;
    private final int fractionalPrecision;

    private static final NullColumn REUSE_NULL = new NullColumn();
    private static final DayTimeColumn EMPTY = DayTimeColumn.from(0L, 0);

    public DayTimeColumnSerializer(DayTimeIntervalType dayTimeIntervalType) {
        this.dayTimeIntervalType = dayTimeIntervalType;
        this.fractionalPrecision = dayTimeIntervalType.getFractionalPrecision();
        this.dayTimeFormat = DayTimeColumn.getDayTimeFormat(dayTimeIntervalType);
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<AbstractBaseColumn> duplicate() {
        return new DayTimeColumnSerializer(dayTimeIntervalType);
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
        DayTimeColumn dayTimeColumn = (DayTimeColumn) from;
        DayTimeColumn res =
                DayTimeColumn.from((Long) dayTimeColumn.getData(), dayTimeColumn.getNanos());
        res.setDayTimeFormat(dayTimeFormat);
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
            target.writeLong(Long.MIN_VALUE);
        } else {
            DayTimeColumn dayTimeColumn = (DayTimeColumn) record;
            target.writeLong((Long) dayTimeColumn.getData());
            if (fractionalPrecision > 3) {
                target.writeInt(dayTimeColumn.getNanos());
            }
        }
    }

    @Override
    public AbstractBaseColumn deserialize(DataInputView source) throws IOException {
        long milliSeconds = source.readLong();
        if (milliSeconds < 0) {
            return REUSE_NULL;
        } else {
            int nanos = 0;
            if (fractionalPrecision > 3) {
                nanos = source.readInt();
            }
            DayTimeColumn res = DayTimeColumn.from(milliSeconds, nanos);
            res.setDayTimeFormat(dayTimeFormat);
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
        long milliSeconds = source.readLong();
        if (milliSeconds < 0) {
            target.writeLong(Long.MIN_VALUE);
        } else {
            target.writeLong(milliSeconds);
            if (fractionalPrecision > 3) {
                target.writeInt(source.readInt());
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DayTimeColumnSerializer) {
            DayTimeColumnSerializer that = (DayTimeColumnSerializer) obj;
            return that.dayTimeIntervalType.equals(this.dayTimeIntervalType);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public TypeSerializerSnapshot<AbstractBaseColumn> snapshotConfiguration() {
        return new DayTimeColumnSerializerSnapshot(dayTimeIntervalType);
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class DayTimeColumnSerializerSnapshot
            implements TypeSerializerSnapshot<AbstractBaseColumn> {

        private static final int CURRENT_VERSION = 3;

        private DayTimeIntervalType dayTimeIntervalType;

        @SuppressWarnings("unused")
        public DayTimeColumnSerializerSnapshot() {
            // this constructor is used when restoring from a checkpoint/savepoint.
        }

        public DayTimeColumnSerializerSnapshot(DayTimeIntervalType dayTimeIntervalType) {
            this.dayTimeIntervalType = dayTimeIntervalType;
        }

        @Override
        public int getCurrentVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            out.writeByte(dayTimeIntervalType.getDayPrecision());
            out.writeByte(dayTimeIntervalType.getFractionalPrecision());
            switch (dayTimeIntervalType.getResolution()) {
                case DAY:
                    out.writeByte(1);
                    break;
                case DAY_TO_HOUR:
                    out.writeByte(2);
                    break;
                case DAY_TO_MINUTE:
                    out.writeByte(3);
                    break;
                case DAY_TO_SECOND:
                    out.writeByte(4);
                    break;
                case HOUR:
                    out.writeByte(5);
                    break;
                case HOUR_TO_MINUTE:
                    out.writeByte(6);
                    break;
                case HOUR_TO_SECOND:
                    out.writeByte(7);
                    break;
                case MINUTE:
                    out.writeByte(8);
                    break;
                case MINUTE_TO_SECOND:
                    out.writeByte(9);
                    break;
                case SECOND:
                    out.writeByte(10);
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Unsupported resolution:" + dayTimeIntervalType.getResolution());
            }
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            DayTimeIntervalType.DayTimeResolution resolution;
            int dayPrecision = in.readByte();
            int fractionalPrecision = in.readByte();
            int num = in.readByte();
            switch (num) {
                case 1:
                    resolution = DayTimeIntervalType.DayTimeResolution.DAY;
                    break;
                case 2:
                    resolution = DayTimeIntervalType.DayTimeResolution.DAY_TO_HOUR;
                    break;
                case 3:
                    resolution = DayTimeIntervalType.DayTimeResolution.DAY_TO_MINUTE;
                    break;
                case 4:
                    resolution = DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND;
                    break;
                case 5:
                    resolution = DayTimeIntervalType.DayTimeResolution.HOUR;
                    break;
                case 6:
                    resolution = DayTimeIntervalType.DayTimeResolution.HOUR_TO_MINUTE;
                    break;
                case 7:
                    resolution = DayTimeIntervalType.DayTimeResolution.HOUR_TO_SECOND;
                    break;
                case 8:
                    resolution = DayTimeIntervalType.DayTimeResolution.MINUTE;
                    break;
                case 9:
                    resolution = DayTimeIntervalType.DayTimeResolution.MINUTE_TO_SECOND;
                    break;
                case 10:
                    resolution = DayTimeIntervalType.DayTimeResolution.SECOND;
                    break;
                default:
                    throw new IllegalArgumentException("An unexpected value:" + num);
            }
            dayTimeIntervalType =
                    new DayTimeIntervalType(resolution, dayPrecision, fractionalPrecision);
        }

        @Override
        public TypeSerializer<AbstractBaseColumn> restoreSerializer() {
            return new DayTimeColumnSerializer(dayTimeIntervalType);
        }

        @Override
        public TypeSerializerSchemaCompatibility<AbstractBaseColumn> resolveSchemaCompatibility(
                TypeSerializer<AbstractBaseColumn> newSerializer) {
            if (!(newSerializer instanceof DayTimeColumnSerializer)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            DayTimeColumnSerializer that = (DayTimeColumnSerializer) newSerializer;
            if (!this.dayTimeIntervalType.equals(that.dayTimeIntervalType)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            } else {
                return TypeSerializerSchemaCompatibility.compatibleAsIs();
            }
        }
    }
}
