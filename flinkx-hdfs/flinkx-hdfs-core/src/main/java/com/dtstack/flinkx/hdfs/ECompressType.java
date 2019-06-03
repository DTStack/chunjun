/**
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

package com.dtstack.flinkx.hdfs;

/**
 * @author jiangbo
 * @explanation
 * @date 2019/4/3
 */
public enum ECompressType {

    /**
     * .gz
     */
    GZIP("GZIP", ".gz"),

    /**
     * .bz2
     */
    BZIP2("BZIP2", ".bz2"),

    SNAPPY("SNAPPY", null),

    NONE("NONE", null);

    private String type;

    private String suffix;

    ECompressType(String type, String suffix) {
        this.type = type;
        this.suffix = suffix;
    }

    public static ECompressType getByType(String type){
        for (ECompressType value : ECompressType.values()) {
            if (value.getType().equalsIgnoreCase(type)){
                return value;
            }
        }

        throw new IllegalArgumentException("Unsupported compress type: " + type);
    }

    public String getType() {
        return type;
    }

    public String getSuffix() {
        return suffix;
    }
}
