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

package com.dtstack.chunjun.element;

/** @author liuliu 2022/5/9 */
public class ClassSizeUtil {
    public static final int ColumnRowDataSize;
    public static final int AbstractBaseColumnSize;
    public static final int StringColumnSize;
    public static final int TimestampColumnSize;
    public static final int BytesColumnSize;
    public static final int MapColumnSize;

    public static final int ArraySize;

    public static final int IntSize;
    public static final int StringSize;
    public static final int ByteSize;
    public static final int FloatSize;
    public static final int LongSize;

    // ignore padding
    static {
        int REFERENCE = 4;
        int HEAD = 12;

        IntSize = Integer.SIZE / Byte.SIZE;
        FloatSize = Float.SIZE / Byte.SIZE;
        LongSize = Long.SIZE / Byte.SIZE;
        // ignore char array
        StringSize = HEAD + IntSize * 2 + HEAD + IntSize;
        ByteSize = HEAD;

        ArraySize = HEAD + IntSize;

        // ignore element size
        int ArrayListSize = HEAD + IntSize * 2 + REFERENCE + HEAD + IntSize;

        // ignore table size
        int HashMapSize =
                HEAD + Integer.SIZE / Byte.SIZE * 4 + Float.SIZE / Byte.SIZE + REFERENCE * 3 + HEAD;

        int HashSetSize = HEAD + HashMapSize;

        int RowKindSize = HEAD + StringSize + 1 + 4;

        ColumnRowDataSize =
                HEAD + ArrayListSize + HashMapSize + HashSetSize + RowKindSize + REFERENCE * 4;
        AbstractBaseColumnSize = HEAD + REFERENCE + IntSize;
        StringColumnSize = AbstractBaseColumnSize + REFERENCE * 2;
        TimestampColumnSize = AbstractBaseColumnSize + IntSize;
        BytesColumnSize = AbstractBaseColumnSize + ArraySize + REFERENCE;
        MapColumnSize = AbstractBaseColumnSize + HashMapSize;
    }

    public static long align(int num) {
        // The 7 comes from that the alignSize is 8 which is the number of bytes stored and sent
        // together
        return (num >> 3) << 3;
    }

    public static int getStringMemory(String str) {
        if (str == null) {
            return 0;
        }
        return str.length() * 2 + StringSize;
    }

    public static int getStringSize(String str) {
        if (str == null) {
            return 0;
        }
        return str.length();
    }
}
