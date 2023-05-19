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

package com.dtstack.chunjun.config;

import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.util.TypeConfigUtil;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;

import java.io.Serializable;
import java.util.Locale;

/** @author liuliu 2022/8/31 */
public class TypeConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private String type;
    private String suffixType;
    private Integer precision;
    private Integer scale;

    public TypeConfig() {}

    public static TypeConfig of(String type) {
        TypeConfig typeConfig = new TypeConfig();
        typeConfig.setType(type);

        return typeConfig;
    }

    public TypeConfig(String prefixType, String suffixType, Integer precision, Integer scale) {
        this.type = prefixType.toUpperCase(Locale.ENGLISH).trim();
        this.suffixType = suffixType.toUpperCase(Locale.ENGLISH).trim();
        this.precision = precision;
        this.scale = scale;
    }

    public TypeConfig(String type, Integer precision, Integer scale) {
        this.type = type.toUpperCase(Locale.ENGLISH).trim();
        this.precision = precision;
        this.scale = scale;
    }

    public String getType() {
        if (suffixType != null) {
            return type + " " + suffixType;
        }
        return type;
    }

    public void setType(String type) {
        this.type = type.toUpperCase(Locale.ENGLISH);
    }

    public Integer getPrecision() {
        return precision;
    }

    public void setPrecision(Integer precision) {
        this.precision = precision;
    }

    public Integer getScale() {
        return scale;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
    }

    public String toOriginTypeStr() {
        StringBuilder builder = new StringBuilder(type);
        if (suffixType != null) {
            if (precision != null) {
                builder.append("(").append(precision).append(")").append(" ").append(suffixType);
                if (scale != null) {
                    builder.append("(").append(scale).append(")");
                }
            }
        } else {
            if (precision != null || scale != null) {
                builder.append("(");
                if (precision != null) {
                    builder.append(precision);
                }
                if (scale != null) {
                    builder.append(",").append(scale);
                }
                builder.append(")");
            }
        }
        return builder.toString();
    }

    public static TypeConfig fromString(String type) {
        try {
            return TypeConfigUtil.getTypeConf(type);
        } catch (Exception e) {
            throw new ChunJunRuntimeException(String.format("failed to analyze type[%s]", type), e);
        }
    }

    public boolean isValidDecimalPrecision() {
        return precision != null && precision > 0 && precision <= 38;
    }

    public boolean isValidDecimalScale() {
        return scale != null && precision != null && scale <= precision && scale >= 0;
    }

    public boolean isValidTimestampPrecision() {
        return precision != null && precision >= 0 && precision <= 9;
    }

    public boolean isValidTimestampScale() {
        return scale != null && scale >= 0 && scale <= 9;
    }

    public boolean isValidYearPrecision() {
        return precision != null
                && precision >= YearMonthIntervalType.MIN_PRECISION
                && precision <= YearMonthIntervalType.MAX_PRECISION;
    }

    public boolean isValidDayPrecision() {
        return precision != null
                && precision >= DayTimeIntervalType.MIN_DAY_PRECISION
                && precision <= DayTimeIntervalType.MAX_DAY_PRECISION;
    }

    public boolean isValidFractionalPrecision(int fractionalPrecision) {
        return fractionalPrecision >= DayTimeIntervalType.MIN_FRACTIONAL_PRECISION
                && fractionalPrecision <= DayTimeIntervalType.MAX_FRACTIONAL_PRECISION;
    }

    public DataType toDecimalDataType() {
        if (isValidDecimalPrecision() && isValidDecimalScale()) {
            return DataTypes.DECIMAL(precision, scale);
        } else {

            return DataTypes.DECIMAL(38, 18);
        }
    }

    public DataType toTimestampDataType() {
        return toTimestampDataType(6);
    }

    public DataType toTimestampDataType(int defaultPrecision) {
        if (isValidTimestampScale()) {
            return DataTypes.TIMESTAMP(scale);
        } else if (isValidTimestampPrecision()) {
            return DataTypes.TIMESTAMP(precision);
        }
        return DataTypes.TIMESTAMP(defaultPrecision);
    }

    public DataType toZonedTimestampDataType() {
        return toZonedTimestampDataType(6);
    }

    public DataType toZonedTimestampDataType(int defaultPrecision) {
        if (isValidTimestampScale()) {
            return DataTypes.TIMESTAMP_WITH_TIME_ZONE(scale);
        } else if (isValidTimestampPrecision()) {
            return DataTypes.TIMESTAMP_WITH_TIME_ZONE(precision);
        }
        return DataTypes.TIMESTAMP_WITH_TIME_ZONE(defaultPrecision);
    }

    public DataType toLocalZonedTimestampDataType() {
        return toLocalZonedTimestampDataType(6);
    }

    public DataType toLocalZonedTimestampDataType(int defaultPrecision) {
        if (isValidTimestampScale()) {
            return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(scale);
        } else if (isValidTimestampPrecision()) {
            return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(precision);
        }
        return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(defaultPrecision);
    }

    public DataType toIntervalYearDataType() {
        return toIntervalYearDataType(YearMonthIntervalType.DEFAULT_PRECISION);
    }

    public DataType toIntervalYearDataType(int defaultYearPrecision) {
        if (isValidYearPrecision()) {
            return DataTypes.INTERVAL(DataTypes.YEAR(precision));
        }
        return DataTypes.INTERVAL(DataTypes.YEAR(defaultYearPrecision));
    }

    public DataType toIntervalYearMonthDataType() {
        return toIntervalYearMonthDataType(YearMonthIntervalType.DEFAULT_PRECISION);
    }

    public DataType toIntervalYearMonthDataType(int defaultYearPrecision) {
        if (isValidYearPrecision()) {
            return DataTypes.INTERVAL(DataTypes.YEAR(precision), DataTypes.MONTH());
        }
        return DataTypes.INTERVAL(DataTypes.YEAR(defaultYearPrecision));
    }

    public DataType toIntervalMonthDataType() {
        return DataTypes.INTERVAL(DataTypes.MONTH());
    }

    public DataType toIntervalDayDataType() {
        return toIntervalDayDataType(DayTimeIntervalType.DEFAULT_DAY_PRECISION);
    }

    public DataType toIntervalDayDataType(int defaultDayPrecision) {
        if (isValidDayPrecision()) {
            return DataTypes.INTERVAL(DataTypes.DAY(precision));
        }
        return DataTypes.INTERVAL(DataTypes.DAY(defaultDayPrecision));
    }

    public DataType toIntervalDayHourDataType() {
        return toIntervalDayHourDataType(DayTimeIntervalType.DEFAULT_DAY_PRECISION);
    }

    public DataType toIntervalDayHourDataType(int defaultDayPrecision) {
        if (isValidDayPrecision()) {
            return DataTypes.INTERVAL(DataTypes.DAY(precision), DataTypes.HOUR());
        }
        return DataTypes.INTERVAL(DataTypes.DAY(defaultDayPrecision), DataTypes.HOUR());
    }

    public DataType toIntervalDayMinuteDataType() {
        return toIntervalDayMinuteDataType(DayTimeIntervalType.DEFAULT_DAY_PRECISION);
    }

    public DataType toIntervalDayMinuteDataType(int defaultDayPrecision) {
        if (isValidDayPrecision()) {
            return DataTypes.INTERVAL(DataTypes.DAY(precision), DataTypes.MINUTE());
        }
        return DataTypes.INTERVAL(DataTypes.DAY(defaultDayPrecision), DataTypes.MINUTE());
    }

    public DataType toIntervalDaySecondDataType() {
        return toIntervalDaySecondDataType(
                DayTimeIntervalType.DEFAULT_DAY_PRECISION,
                DayTimeIntervalType.DEFAULT_FRACTIONAL_PRECISION);
    }

    public DataType toIntervalDaySecondDataType(
            int defaultDayPrecision, int defaultFractionalPrecision) {
        if (isValidDayPrecision()) {
            if (isValidFractionalPrecision(scale)) {
                return DataTypes.INTERVAL(DataTypes.DAY(precision), DataTypes.SECOND(scale));
            }
            return DataTypes.INTERVAL(
                    DataTypes.DAY(precision), DataTypes.SECOND(defaultFractionalPrecision));
        } else {
            if (isValidFractionalPrecision(scale)) {
                return DataTypes.INTERVAL(
                        DataTypes.DAY(defaultDayPrecision), DataTypes.SECOND(scale));
            }
            return DataTypes.INTERVAL(
                    DataTypes.DAY(defaultDayPrecision),
                    DataTypes.SECOND(defaultFractionalPrecision));
        }
    }

    public DataType toIntervalHourDataType() {
        return DataTypes.INTERVAL(DataTypes.HOUR());
    }

    public DataType toIntervalHourMinuteDataType() {
        return DataTypes.INTERVAL(DataTypes.HOUR(), DataTypes.MINUTE());
    }

    public DataType toIntervalHourSecondDataType() {
        return toIntervalHourSecondDataType(DayTimeIntervalType.DEFAULT_FRACTIONAL_PRECISION);
    }

    public DataType toIntervalHourSecondDataType(int defaultFractionalPrecision) {
        if (isValidFractionalPrecision(precision)) {
            return DataTypes.INTERVAL(DataTypes.HOUR(), DataTypes.SECOND(precision));
        }
        return DataTypes.INTERVAL(DataTypes.HOUR(), DataTypes.SECOND(defaultFractionalPrecision));
    }

    public DataType toIntervalMinuteDataType() {
        return DataTypes.INTERVAL(DataTypes.MINUTE());
    }

    public DataType toIntervalMinuteSecondDataType() {
        return toIntervalMinuteSecondDataType(DayTimeIntervalType.DEFAULT_FRACTIONAL_PRECISION);
    }

    public DataType toIntervalMinuteSecondDataType(int defaultFractionalPrecision) {
        if (isValidFractionalPrecision(precision)) {
            return DataTypes.INTERVAL(DataTypes.MINUTE(), DataTypes.SECOND(precision));
        }
        return DataTypes.INTERVAL(DataTypes.MINUTE(), DataTypes.SECOND(defaultFractionalPrecision));
    }

    public DataType toIntervalSecondDataType() {
        return toIntervalSecondDataType(DayTimeIntervalType.DEFAULT_FRACTIONAL_PRECISION);
    }

    public DataType toIntervalSecondDataType(int defaultFractionalPrecision) {
        if (isValidFractionalPrecision(precision)) {
            return DataTypes.INTERVAL(DataTypes.SECOND(precision));
        }
        return DataTypes.INTERVAL(DataTypes.SECOND(defaultFractionalPrecision));
    }

    public DataType toTimeDataType() {
        return toTimeDataType(3);
    }

    public DataType toTimeDataType(int defaultPrecision) {
        if (isValidTimestampScale()) {
            return DataTypes.TIME(scale);
        } else if (isValidTimestampPrecision()) {
            return DataTypes.TIME(precision);
        }
        return DataTypes.TIME(defaultPrecision);
    }

    @Override
    public int hashCode() {
        int result = 1;
        if (type != null) {
            result = 31 * result + type.hashCode();
        }
        if (suffixType != null) {
            result = 31 * result + suffixType.hashCode();
        }
        if (precision != null) {
            result = 31 * result + precision.hashCode();
        }
        if (scale != null) {
            result = 31 * result + scale.hashCode();
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TypeConfig)) {
            return false;
        }
        TypeConfig that = (TypeConfig) obj;
        if (this.type == null) {
            if (that.type != null) {
                return false;
            }
        } else {
            if (!this.type.equals(that.type)) {
                return false;
            }
        }
        if (this.suffixType == null) {
            if (that.suffixType != null) {
                return false;
            }
        } else {
            if (!this.suffixType.equals(that.suffixType)) {
                return false;
            }
        }
        if (this.precision == null) {
            if (that.precision != null) {
                return false;
            }
        } else {
            if (!this.precision.equals(that.precision)) {
                return false;
            }
        }
        if (this.scale == null) {
            return that.scale == null;
        } else {
            return this.scale.equals(that.scale);
        }
    }

    @Override
    public String toString() {
        return "TypeConfig{"
                + "type='"
                + getType()
                + '\''
                + ", precision="
                + precision
                + ", scale="
                + scale
                + '}';
    }
}
