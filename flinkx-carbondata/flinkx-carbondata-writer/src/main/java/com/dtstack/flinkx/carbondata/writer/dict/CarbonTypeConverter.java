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

package com.dtstack.flinkx.carbondata.writer.dict;


import com.dtstack.flinkx.util.DateUtil;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;


/**
 * Carbon Type Converter
 *
 * Company: www.dtstack.com
 * @author huyifan_zju@163.com
 */
public class CarbonTypeConverter {

    private static final Logger LOG = LoggerFactory.getLogger(CarbonTypeConverter.class);

    private static final String HIVE_DEFAULT_PARTITION = "__HIVE_DEFAULT_PARTITION__";

    public static String objectToString(Object value, String serializationNullFormat) {
        return objectToString(value, serializationNullFormat, false);
    }


    /**
     * Return a String representation of the input value
     * @param value input value
     * @param serializationNullFormat string for null value
     * @param isVarcharType whether it is varchar type. A varchar type has no string length limit
     */
    public static String objectToString(Object value, String serializationNullFormat, boolean isVarcharType) {
        if(value == null) {
            return serializationNullFormat;
        } else {
            if(value instanceof String) {
                String s = (String) value;
                if(!isVarcharType && s.length() > CarbonCommonConstants.MAX_CHARS_PER_COLUMN_DEFAULT) {
                    throw new IllegalArgumentException("Dataload failed, String length cannot exceed " + CarbonCommonConstants.MAX_CHARS_PER_COLUMN_DEFAULT + " characters");
                }
                return s;
            }
            if(value instanceof BigDecimal) {
                BigDecimal d = (BigDecimal) value;
                return d.toPlainString();
            }
            if(value instanceof Integer) {
                Integer i = (Integer) value;
                return i.toString();
            }
            if(value instanceof Long) {
                Long l = (Long) value;
                return l.toString();
            }
            if(value instanceof Double) {
                Double d = (Double) value;
                return d.toString();
            }
            if(value instanceof Timestamp) {
                Timestamp t = (Timestamp) value;
                return  DateUtil.getDateTimeFormatter().format(t);
            }
            if(value instanceof java.sql.Date) {
                java.sql.Date d = (java.sql.Date) value;
                return DateUtil.getDateFormatter().format(d);
            }
            if(value instanceof Boolean) {
                Boolean b = (Boolean) value;
                return b.toString();
            }
            if(value instanceof Short) {
                Short s = (Short) value;
                return s.toString();
            }

            if(value instanceof Float) {
                Float f = (Float) value;
                return f.toString();
            }

        }
        return value.toString();
    }


    public static void checkStringType(String s, String serializationNullFormat, DataType dataType) throws ParseException {
        if (s == null || s.length() == 0 || s.equalsIgnoreCase(serializationNullFormat)) {
            return;
        }
        if (dataType == DataTypes.INT) {
            Integer.parseInt(s);
        } else if (dataType == DataTypes.LONG) {
            Long.parseLong(s);
        } else if (dataType == DataTypes.DATE) {
            DateUtil.getDateFormatter().parse(s);
        } else if (dataType == DataTypes.TIMESTAMP) {
            DateUtil.getDateTimeFormatter().parse(s);
        } else if (dataType == DataTypes.SHORT || dataType == DataTypes.SHORT_INT) {
            Short.parseShort(s);
        } else if (dataType == DataTypes.BOOLEAN) {
            Boolean.valueOf(s);
        } else if (DataTypes.isDecimal(dataType)) {
            Double.valueOf(s);
        } else if (dataType == DataTypes.FLOAT) {
            Float.valueOf(s);
        } else if (dataType == DataTypes.DOUBLE) {
            Double.valueOf(s);
        } else if (dataType == DataTypes.STRING || dataType == DataTypes.VARCHAR) {
            // IT'S OK
        } else {
            throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }

    }

    /**
     * Update partition values as per the right date and time format
     * @return updated partition spec
     */
    public static Map<String,String> updatePartitions(Map<String,String> partitionSpec, CarbonTable table) {
        CacheProvider cacheProvider = CacheProvider.getInstance();
        Cache<DictionaryColumnUniqueIdentifier, Dictionary> forwardDictionaryCache = cacheProvider.createCache(CacheType.FORWARD_DICTIONARY);
        Map<String,String> map = new HashMap<>((partitionSpec.size()<<2)/3);
        for (Map.Entry<String,String> entry : partitionSpec.entrySet()) {
            String col = entry.getKey();
            String pvalue = entry.getValue();
            String value = pvalue;
            if (pvalue == null) {
                value = HIVE_DEFAULT_PARTITION;
            } else if (pvalue.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL)) {
                value = "";
            }
            CarbonColumn carbonColumn = table.getColumnByName(table.getTableName(), col.toLowerCase());
            try {
                if(value.equals(HIVE_DEFAULT_PARTITION)) {
                    map.put(col, value);
                } else {
                    String convertedString = convertToCarbonFormat(value, carbonColumn, forwardDictionaryCache, table);
                    if(convertedString == null) {
                        map.put(col, HIVE_DEFAULT_PARTITION);
                    } else {
                        map.put(col, convertedString);
                    }
                }
            } catch (Exception e) {
                LOG.error(e.getMessage());
                map.put(col, value);
            }
        }
        Map<String,String> ret = new HashMap<>((map.size()<<2)/3);
        for(Map.Entry<String,String> entry : map.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            ret.put(ExternalCatalogUtils.escapePathName(key), ExternalCatalogUtils.escapePathName(value));
        }
        return ret;
    }

    public static String convertToCarbonFormat(String value, CarbonColumn column, Cache<DictionaryColumnUniqueIdentifier,Dictionary> forwardDictionaryCache, CarbonTable table) throws IOException {
        if(column.hasEncoding(Encoding.DICTIONARY)) {
            if (column.hasEncoding(Encoding.DIRECT_DICTIONARY)) {
                if (column.getDataType().equals(DataTypes.TIMESTAMP)) {
                    Object time = DirectDictionaryKeyGeneratorFactory.getDirectDictionaryGenerator(
                            column.getDataType(),
                            CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT
                    ).getValueFromSurrogate(Integer.parseInt(value));
                    if(time == null) {
                        return null;
                    }
                    return DateTimeUtils.timestampToString(Long.valueOf(time.toString()) * 1000);
                } else if (column.getDataType().equals(DataTypes.DATE)) {
                    Object date = DirectDictionaryKeyGeneratorFactory.getDirectDictionaryGenerator(
                            column.getDataType(),
                            CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT
                    ).getValueFromSurrogate(Integer.parseInt(value));
                    if(date == null) {
                        return null;
                    }
                    return DateTimeUtils.dateToString(Integer.valueOf(date.toString()));
                }
            }
            String dictionaryPath = table.getTableInfo().getFactTable().getTableProperties().get(CarbonCommonConstants.DICTIONARY_PATH);
            DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier = new DictionaryColumnUniqueIdentifier(
                    table.getAbsoluteTableIdentifier(),
                    column.getColumnIdentifier(), column.getDataType(),
                    dictionaryPath);
            return forwardDictionaryCache.get(
                    dictionaryColumnUniqueIdentifier).getDictionaryValueForKey(Integer.parseInt(value));
        }
        try {
            DataType dataType = column.getDataType();
            if(dataType == DataTypes.TIMESTAMP) {
                return DateTimeUtils.timestampToString(Long.parseLong(value) * 1000);
            } else if(dataType == DataTypes.DATE) {
                return DateTimeUtils.dateToString(DateTimeUtils.millisToDays(Long.parseLong(value)));
            }
            return value;
        } catch(Exception e) {
            LOG.error(e.getMessage());
            return value;
        }

    }

    public static Object string2col(String s, DataType dataType, String serializationNullFormat, SimpleDateFormat timeStampFormat, SimpleDateFormat dateFormat) throws ParseException {
        if (s == null || s.length() == 0 || s.equalsIgnoreCase(serializationNullFormat)) {
            return null;
        }
        if (dataType == DataTypes.INT) {
            return Integer.parseInt(s);
        } else if (dataType == DataTypes.LONG) {
            return Long.parseLong(s);
        } else if (dataType == DataTypes.DATE) {
            return dateFormat.parse(s);
        } else if (dataType == DataTypes.TIMESTAMP) {
            return timeStampFormat.parse(s);
        } else if (dataType == DataTypes.SHORT || dataType == DataTypes.SHORT_INT) {
            return Short.parseShort(s);
        } else if (dataType == DataTypes.BOOLEAN) {
            return Boolean.valueOf(s);
        } else if (DataTypes.isDecimal(dataType)) {
            return new BigDecimal(s);
        } else if (dataType == DataTypes.FLOAT) {
            return Float.valueOf(s);
        } else if (dataType == DataTypes.DOUBLE) {
            return Double.valueOf(s);
        } else if (dataType == DataTypes.STRING || dataType == DataTypes.VARCHAR) {
            return s;
        } else {
            throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
    }

}
