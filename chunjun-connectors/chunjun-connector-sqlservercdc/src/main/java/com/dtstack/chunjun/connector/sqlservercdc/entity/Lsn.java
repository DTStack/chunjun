/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dtstack.chunjun.connector.sqlservercdc.entity;

import com.dtstack.chunjun.util.StringUtil;

import java.util.Arrays;

public class Lsn implements Comparable<Lsn> {

    private static final String NULL_STRING = "NULL";

    public static final Lsn NULL = new Lsn(null);

    private final byte[] binary;
    private int[] unsignedBinary;

    private String string;

    private Lsn(byte[] binary) {
        this.binary = binary;
    }

    public byte[] getBinary() {
        return binary;
    }

    public boolean isAvailable() {
        return binary != null;
    }

    private int[] getUnsignedBinary() {
        if (unsignedBinary != null || binary == null) {
            return unsignedBinary;
        }

        unsignedBinary = new int[binary.length];
        for (int i = 0; i < binary.length; i++) {
            unsignedBinary[i] = Byte.toUnsignedInt(binary[i]);
        }
        return unsignedBinary;
    }

    @Override
    public String toString() {
        if (string != null) {
            return string;
        }
        final StringBuilder sb = new StringBuilder();
        if (binary == null) {
            return NULL_STRING;
        }

        final int[] unsigned = getUnsignedBinary();
        if (null == unsigned) {
            return NULL_STRING;
        }

        for (int i = 0; i < unsigned.length; i++) {
            final String byteStr = Integer.toHexString(unsigned[i]);
            if (byteStr.length() == 1) {
                sb.append('0');
            }
            sb.append(byteStr);
            if (i == 3 || i == 7) {
                sb.append(':');
            }
        }
        string = sb.toString();
        return string;
    }

    public static Lsn valueOf(String lsnString) {
        return (lsnString == null || NULL_STRING.equals(lsnString))
                ? NULL
                : new Lsn(StringUtil.hexStringToByteArray(lsnString.replace(":", "")));
    }

    public static Lsn valueOf(byte[] lsnBinary) {
        return (lsnBinary == null) ? NULL : new Lsn(lsnBinary);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(binary);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Lsn other = (Lsn) obj;
        if (!Arrays.equals(binary, other.binary)) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(Lsn o) {
        if (this == o) {
            return 0;
        }

        if (!this.isAvailable() || !o.isAvailable()) {
            throw new IllegalArgumentException("object can not be null!");
        }

        final int[] thisU = getUnsignedBinary();
        final int[] thatU = o.getUnsignedBinary();
        if (null != thisU && null != thatU) {
            for (int i = 0; i < thisU.length; i++) {
                final int diff = thisU[i] - thatU[i];
                if (diff != 0) {
                    return diff;
                }
            }
        }

        return 0;
    }
}
