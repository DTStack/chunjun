package com.dtstack.flinkx.carbondata.writer;


import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CarbonTypeConverter {

    private static final Logger LOG = LoggerFactory.getLogger(CarbonTypeConverter.class);

    private static final String HIVE_DEFAULT_PARTITION = "__HIVE_DEFAULT_PARTITION__";

    public static DataType convertToConbonDataType(String type) {
        type = type.toUpperCase();

        switch (type) {
            case "STRING":
                return DataTypes.STRING;
            case "DATE":
                return DataTypes.DATE;
            case "TIMESTAMP":
                return DataTypes.TIMESTAMP;
            case "BOOLEAN":
                return DataTypes.BOOLEAN;
            case "SMALLINT":
            case "TINYINT":
                return DataTypes.SHORT;
            case "INT":
                return DataTypes.INT;
            case "FLOAT":
                return DataTypes.FLOAT;
            case "BIGINT":
                return DataTypes.LONG;
            case "DOUBLE":
            case "NUMERIC":
                return DataTypes.DOUBLE;
            default:
                throw new IllegalArgumentException("Unsupported DataType: " + type);
        }
    }


    public static String objectToString(Object value, String serializationNullFormat, SimpleDateFormat timeStampFormat, SimpleDateFormat dateFormat) {
        return objectToString(value, serializationNullFormat, timeStampFormat, dateFormat, false);
    }


    /**
     * Return a String representation of the input value
     * @param value input value
     * @param serializationNullFormat string for null value
     * @param timeStampFormat timestamp format
     * @param dateFormat date format
     * @param isVarcharType whether it is varchar type. A varchar type has no string length limit
     */
    public static String objectToString(Object value, String serializationNullFormat, SimpleDateFormat timeStampFormat, SimpleDateFormat dateFormat, boolean isVarcharType) {
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
                return timeStampFormat.format(t);
            }
            if(value instanceof java.sql.Date) {
                java.sql.Date d = (java.sql.Date) value;
                return dateFormat.format(d);
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
        throw new IllegalArgumentException("Unsupported type for: " + value);
    }


    public static void checkStringType(String s, String serializationNullFormat, SimpleDateFormat timeStampFormat, SimpleDateFormat dateFormat, DataType dataType) throws ParseException {
        if (s == null || s.length() == 0 || s.equalsIgnoreCase(serializationNullFormat)) {
            return;
        }
        if (dataType == DataTypes.INT) {
            Integer.parseInt(s);
        } else if (dataType == DataTypes.LONG) {
            Long.parseLong(s);
        } else if (dataType == DataTypes.DATE) {
            dateFormat.parse(s);
        } else if (dataType == DataTypes.TIMESTAMP) {
            timeStampFormat.parse(s);
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
        Map<String,String> map = new HashMap<>();
        for (Map.Entry<String,String> entry : map.entrySet()) {
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
        return map;
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



}
