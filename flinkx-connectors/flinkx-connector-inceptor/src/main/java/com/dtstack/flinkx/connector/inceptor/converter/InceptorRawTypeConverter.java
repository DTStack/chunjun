package com.dtstack.flinkx.connector.inceptor.converter;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.throwable.UnsupportedTypeException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.util.Locale;

public class InceptorRawTypeConverter {

    public static DataType apply(String type) {
        type = type.toUpperCase(Locale.ENGLISH);
        int left = type.indexOf(ConstantValue.LEFT_PARENTHESIS_SYMBOL);
        int right = type.indexOf(ConstantValue.RIGHT_PARENTHESIS_SYMBOL);
        String leftStr = type;
        String rightStr = null;
        if (left > 0 && right > 0) {
            leftStr = type.substring(0, left);
            rightStr = type.substring(left + 1, type.length() - 1);
        }

        switch (leftStr) {
            case "BOOLEAN":
                return DataTypes.BOOLEAN();
            case "TINYINT":
                return DataTypes.TINYINT();
            case "SMALLINT":
                return DataTypes.SMALLINT();
            case "INT":
            case "INTEGER":
                return DataTypes.INT();
            case "BIGINT":
                return DataTypes.BIGINT();
            case "FLOAT":
                return DataTypes.FLOAT();
            case "DECIMAL":
                if (rightStr != null) {
                    String[] split = rightStr.split(ConstantValue.COMMA_SYMBOL);
                    if (split.length == 2) {
                        return DataTypes.DECIMAL(
                                Integer.parseInt(split[0].trim()),
                                Integer.parseInt(split[1].trim()));
                    }
                }
            case "DOUBLE":
                return DataTypes.DOUBLE();
            case "CHAR":
            case "VARCHAR":
            case "STRING":
                return DataTypes.STRING();
            case "DATE":
                return DataTypes.DATE();
            case "TIME":
                return DataTypes.TIME();
            case "TIMESTAMP":
                if (rightStr != null) {
                    String[] split = rightStr.split(ConstantValue.COMMA_SYMBOL);
                    if (split.length == 1) {
                        return DataTypes.TIMESTAMP(Integer.parseInt(split[0].trim()));
                    }
                }
                return DataTypes.TIMESTAMP(6);
            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
