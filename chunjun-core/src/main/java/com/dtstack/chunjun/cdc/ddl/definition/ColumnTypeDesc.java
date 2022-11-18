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

package com.dtstack.chunjun.cdc.ddl.definition;

import com.dtstack.chunjun.cdc.ddl.ColumnType;

public class ColumnTypeDesc {
    private final ColumnType columnType;
    /** 默认长度 * */
    private final Integer defaultPrecision;

    /** 默认小数位数 * */
    private final Integer defaultScale;

    private final Integer maxPrecision;
    private final Integer minPrecision;

    private final Integer maxScale;
    private final Integer minScale;

    private final boolean needPrecision;

    public ColumnTypeDesc(ColumnType columnType, boolean needPrecision) {
        this(columnType, null, null, null, null, null, null, needPrecision);
    }

    public ColumnTypeDesc(
            ColumnType columnType,
            Integer defaultPrecision,
            Integer defaultScale,
            Integer maxPrecision,
            Integer maxScale,
            Integer minPrecision,
            Integer minScale,
            boolean needPrecision) {
        this.columnType = columnType;
        this.defaultPrecision = defaultPrecision;
        this.defaultScale = defaultScale;
        this.maxPrecision = maxPrecision;
        this.maxScale = maxScale;
        this.minPrecision = minPrecision;
        this.minScale = minScale;
        this.needPrecision = needPrecision;
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    public Integer getDefaultPrecision() {
        return defaultPrecision;
    }

    public Integer getDefaultScale() {
        return defaultScale;
    }

    public Integer getMaxPrecision() {
        return maxPrecision;
    }

    public Integer getMinPrecision() {
        return minPrecision;
    }

    public Integer getMinScale() {
        return minScale;
    }

    public Integer getMaxScale() {
        return maxScale;
    }

    public Boolean getNeedPrecision() {
        return needPrecision;
    }

    public Integer getPrecision(Integer precision) {
        if (!needPrecision) {
            return null;
        }
        if (precision == null) {
            return precision;
        }
        if (maxPrecision != null && minPrecision != null) {
            if (precision <= maxPrecision && precision >= minPrecision) {
                return precision;
            }
        }

        if (maxPrecision != null) {
            if (precision > maxPrecision) {
                return defaultPrecision;
            }
        }

        if (minPrecision != null) {
            if (precision < minPrecision) {
                return defaultPrecision;
            }
        }

        return precision;
    }

    public Integer getScale(Integer scale) {
        if (!needPrecision) {
            return null;
        }

        if (scale == null) {
            return scale;
        }
        if (maxScale != null && minScale != null && scale <= maxScale && scale >= minScale) {
            return scale;
        }
        if (maxScale != null && scale > maxScale) {
            return defaultScale;
        }

        if (minScale != null && scale < minScale) {
            return defaultScale;
        }

        return scale;
    }

    public Integer getDefaultConvertPrecision() {
        if (!needPrecision) {
            return null;
        }
        if (maxPrecision != null) {
            return maxPrecision;
        }

        if (defaultPrecision != null) {
            return defaultPrecision;
        }
        return null;
    }

    public Integer getDefaultConvertScale(int precision) {
        if (!needPrecision) {
            return null;
        }

        if (maxScale != null && maxScale < precision) {
            return maxScale;
        }
        if (defaultScale != null && defaultScale < precision) {
            return defaultScale;
        }
        return null;
    }
}
