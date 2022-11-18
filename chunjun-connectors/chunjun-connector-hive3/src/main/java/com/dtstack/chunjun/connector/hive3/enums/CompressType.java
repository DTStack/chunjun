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
package com.dtstack.chunjun.connector.hive3.enums;

import org.apache.commons.lang3.StringUtils;

public enum CompressType {
    /** text file */
    TEXT_GZIP("GZIP", "text", ".gz", 0.331F),
    TEXT_BZIP2("BZIP2", "text", ".bz2", 0.259F),
    TEXT_NONE("NONE", "text", "", 0.637F),

    /** orc file */
    ORC_SNAPPY("SNAPPY", "orc", ".snappy", 0.233F),
    ORC_GZIP("GZIP", "orc", ".gz", 1.0F),
    ORC_BZIP("BZIP", "orc", ".bz", 1.0F),
    ORC_LZ4("LZ4", "orc", ".lz4", 1.0F),
    ORC_NONE("NONE", "orc", "", 0.233F),

    /** parquet file */
    PARQUET_SNAPPY("SNAPPY", "parquet", ".snappy", 0.274F),
    PARQUET_GZIP("GZIP", "parquet", ".gz", 1.0F),
    PARQUET_LZO("LZO", "parquet", ".lzo", 1.0F),
    PARQUET_NONE("NONE", "parquet", "", 1.0F);

    private final String type;

    private final String fileType;

    private final String suffix;

    private final float deviation;

    CompressType(String type, String fileType, String suffix, float deviation) {
        this.type = type;
        this.fileType = fileType;
        this.suffix = suffix;
        this.deviation = deviation;
    }

    public static CompressType getByTypeAndFileType(String type, String fileType) {
        if (StringUtils.isEmpty(type)) {
            if ("PARQUET".equalsIgnoreCase(fileType)) {
                type = "SNAPPY";
            } else {
                type = "NONE";
            }
        }

        for (CompressType value : CompressType.values()) {
            if (value.getType().equalsIgnoreCase(type)
                    && value.getFileType().equalsIgnoreCase(fileType)) {
                return value;
            }
        }

        throw new IllegalArgumentException("No enum constant " + type);
    }

    public String getType() {
        return type;
    }

    public String getFileType() {
        return fileType;
    }

    public String getSuffix() {
        return suffix;
    }

    public float getDeviation() {
        return deviation;
    }
}
