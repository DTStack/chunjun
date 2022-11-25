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

package com.dtstack.chunjun.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.flink.shaded.guava30.com.google.common.base.Splitter;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;

public class DataTypeUtil {

    private static final Pattern COMPOSITE_TYPE_PATTERN = Pattern.compile("(.+?)<(.+)>");
    private static final String ARRAY = "ARRAY";
    private static final String MAP = "MAP";
    private static final String ROW = "ROW";
    private static final char FIELD_DELIMITER = ',';
    private static final char TYPE_DELIMITER = ' ';

    private DataTypeUtil() {}

    /**
     * 目前ARRAY里只支持ROW和其他基本类型
     *
     * @param arrayTypeString
     * @return
     */
    public static TypeInformation convertToArray(String arrayTypeString) {
        Matcher matcher = matchCompositeType(arrayTypeString);
        final String errorMsg = arrayTypeString + "convert to array type error!";
        Preconditions.checkState(matcher.find(), errorMsg);

        String normalizedType = normalizeType(matcher.group(1));
        Preconditions.checkState(ARRAY.equals(normalizedType), errorMsg);

        String elementTypeString = matcher.group(2);
        TypeInformation elementType;
        String normalizedElementType = normalizeType(elementTypeString);
        if (normalizedElementType.startsWith(ROW)) {
            elementType = convertToRow(elementTypeString);
        } else {
            elementType = convertToAtomicType(elementTypeString);
        }

        return Types.OBJECT_ARRAY(elementType);
    }

    /**
     * 目前Map里只支持基本类型
     *
     * @param mapTypeString
     * @return
     */
    public static TypeInformation convertToMap(String mapTypeString) {
        Matcher matcher = matchCompositeType(mapTypeString);
        final String errorMsg = mapTypeString + "convert to map type error!";
        Preconditions.checkState(matcher.find(), errorMsg);

        String normalizedType = normalizeType(matcher.group(1));
        Preconditions.checkState(MAP.equals(normalizedType), errorMsg);

        String kvTypeString = matcher.group(2);
        String[] kvTypeStringList = StringUtils.split(kvTypeString, ",");
        final String mapTypeErrorMsg =
                "There can only be key and value two types in map declaration.";
        Preconditions.checkState(kvTypeStringList.length == 2, mapTypeErrorMsg);
        String keyTypeString = normalizeType(kvTypeStringList[0]);
        String valueTypeString = normalizeType(kvTypeStringList[1]);
        TypeInformation keyType = convertToAtomicType(keyTypeString);
        TypeInformation valueType = convertToAtomicType(valueTypeString);
        return Types.MAP(keyType, valueType);
    }

    /**
     * 目前ROW里只支持基本类型
     *
     * @param rowTypeString
     */
    public static RowTypeInfo convertToRow(String rowTypeString) {
        Matcher matcher = matchCompositeType(rowTypeString);
        final String errorMsg = rowTypeString + "convert to row type error!";
        Preconditions.checkState(matcher.find(), errorMsg);

        String normalizedType = normalizeType(matcher.group(1));
        Preconditions.checkState(ROW.equals(normalizedType), errorMsg);

        String elementTypeStr = matcher.group(2);
        Iterable<String> fieldInfoStrs = splitCompositeTypeField(elementTypeStr);
        Tuple2<TypeInformation[], String[]> info = genFieldInfo(fieldInfoStrs);
        return new RowTypeInfo(info.f0, info.f1);
    }

    /**
     * 获取字段类型 java数据类型转换成DataType
     *
     * @param fieldClassList
     * @return
     */
    public static DataType[] getFieldTypes(List<Class> fieldClassList) {
        DataType[] dataTypes = new DataType[fieldClassList.size()];
        for (int i = 0; i < fieldClassList.size(); i++) {
            if (StringUtils.equalsIgnoreCase(
                    BigDecimal.class.getName(), fieldClassList.get(i).getName())) {
                dataTypes[i] = DECIMAL(DecimalType.MAX_PRECISION, 18);
                continue;
            }
            if (StringUtils.equalsIgnoreCase(
                    Timestamp.class.getName(), fieldClassList.get(i).getName())) {
                dataTypes[i] = TIMESTAMP(3).bridgedTo(Timestamp.class);
                continue;
            }
            dataTypes[i] = TypeConversions.fromClassToDataType(fieldClassList.get(i)).get();
        }
        return dataTypes;
    }

    /**
     * class 转成 TypeInformation
     *
     * @param fieldTypes
     * @return
     */
    public static TypeInformation[] transformTypes(Class[] fieldTypes) {
        TypeInformation[] types = new TypeInformation[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            types[i] = TypeInformation.of(fieldTypes[i]);
        }

        return types;
    }

    private static Tuple2<TypeInformation[], String[]> genFieldInfo(
            Iterable<String> fieldInfoStrs) {
        ArrayList<TypeInformation> types = Lists.newArrayList();
        ArrayList<String> fieldNames = Lists.newArrayList();

        for (String fieldStr : fieldInfoStrs) {
            Iterable<String> splitedInfo = splitTypeInfo(fieldStr);
            ArrayList<String> info = Lists.newArrayList(splitedInfo.iterator());
            Preconditions.checkState(info.size() == 2, "field info must be name with type");

            fieldNames.add(info.get(0));
            TypeInformation fieldType = convertToAtomicType(info.get(1));
            types.add(fieldType);
        }

        TypeInformation[] typeArray = types.toArray(new TypeInformation[types.size()]);
        String[] fieldNameArray = fieldNames.toArray(new String[fieldNames.size()]);
        return Tuple2.of(typeArray, fieldNameArray);
    }

    /**
     * 转换基本类型，所有类型参考Flink官方文档，一共12个基本类型。
     *
     * @param string
     * @return
     */
    public static TypeInformation convertToAtomicType(String string) {
        switch (normalizeType(string)) {
            case "VARCHAR":
            case "STRING":
                return Types.STRING();
            case "BOOLEAN":
                return Types.BOOLEAN();
            case "TINYINT":
                return Types.BYTE();
            case "SMALLINT":
                return Types.SHORT();
            case "INT":
            case "INTEGER":
                return Types.INT();
            case "BIGINT":
                return Types.LONG();
            case "FLOAT":
            case "REAL":
                return Types.FLOAT();
            case "DOUBLE":
                return Types.DOUBLE();
            case "DECIMAL":
            case "DEC":
            case "NUMERIC":
                return Types.DECIMAL();
            case "DATE":
                return Types.SQL_DATE();
            case "TIME":
                return Types.SQL_TIME();
            case "TIMESTAMP":
                return Types.SQL_TIMESTAMP();
            default:
                throw new RuntimeException(
                        "type " + string + "not supported, please refer to the flink doc!");
        }
    }

    /**
     * 补全primaryKey中存在，但是在fields不存在的字段的类型，默认为String
     *
     * @param fieldNames
     * @param fieldTypes
     * @return
     */
    private static Class[] completionFieldTypes(String[] fieldNames, Class[] fieldTypes) {
        if (fieldNames.length == fieldTypes.length) {
            return fieldTypes;
        }
        Class[] compFieldTypes = new Class[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            if (i >= fieldTypes.length) {
                compFieldTypes[i] = String.class;
                continue;
            }
            compFieldTypes[i] = fieldTypes[i];
        }
        return compFieldTypes;
    }

    private static Iterable<String> splitTypeInfo(String string) {
        return Splitter.on(TYPE_DELIMITER).trimResults().omitEmptyStrings().split(string);
    }

    private static Iterable<String> splitCompositeTypeField(String string) {
        return Splitter.on(FIELD_DELIMITER).trimResults().split(string);
    }

    private static String replaceBlank(String s) {
        return s.replaceAll("\\s", " ").trim();
    }

    private static Matcher matchCompositeType(String s) {
        return COMPOSITE_TYPE_PATTERN.matcher(replaceBlank(s));
    }

    private static String normalizeType(String s) {
        return s.toUpperCase().trim();
    }
}
