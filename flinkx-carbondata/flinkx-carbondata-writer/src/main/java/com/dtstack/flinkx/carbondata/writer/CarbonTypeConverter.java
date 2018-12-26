package com.dtstack.flinkx.carbondata.writer;


import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.DataType;

public class CarbonTypeConverter {



    private static final String HIVE_DEFAULT_PARTITION = "__HIVE_DEFAULT_PARTITION__";



    private CarbonTypeConverter() {

    }

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


}
