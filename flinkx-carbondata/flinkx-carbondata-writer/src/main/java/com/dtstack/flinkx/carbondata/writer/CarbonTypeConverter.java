package com.dtstack.flinkx.carbondata.writer;


import java.text.SimpleDateFormat;
import java.util.Date;
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

    public static String convertToDateAndTimeFormats(String value, DataType dataType, SimpleDateFormat timeStampFormat, SimpleDateFormat dateFormat) {
        boolean defaultValue = value != null && value.equalsIgnoreCase(HIVE_DEFAULT_PARTITION);
        try {
            if(dataType == DataTypes.TIMESTAMP && timeStampFormat != null) {
                if(defaultValue) {
                    return timeStampFormat.format(new Date());
                } else {
                    //return timeStampFormat.format(DateTimeUtils.stringToTime(value));
                    return null;
                }
            }
        } catch(Exception e) {
            throw new RuntimeException("Value $value with datatype $dataType on static partition is not correct");
        }
        return null;
    }

    public static String convertToCarbonFormat(String value) {
        return null;
    }


}
