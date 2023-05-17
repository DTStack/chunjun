package com.dtstack.chunjun.connector.hudi.converter;

import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BinaryType;

import java.util.Locale;

public class HudiRowDataMapping {
    public static DataType apply(String type) throws UnsupportedTypeException {
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
                return DataTypes.INT();
            case "BIGINT":
                return DataTypes.BIGINT();
            case "FLOAT":
                return DataTypes.FLOAT();
            case "DOUBLE":
                return DataTypes.DOUBLE();
            case "DECIMAL":
                if (rightStr != null) {
                    String[] split = rightStr.split(ConstantValue.COMMA_SYMBOL);
                    if (split.length == 2) {
                        return DataTypes.DECIMAL(
                                Integer.parseInt(split[0].trim()),
                                Integer.parseInt(split[1].trim()));
                    }
                }
                return DataTypes.DECIMAL(38, 18);
            case "STRING":
            case "VARCHAR":
            case "CHAR":
                return DataTypes.STRING();
            case "BINARY":
                return DataTypes.BINARY(BinaryType.DEFAULT_LENGTH);
            case "TIMESTAMP":
                if (rightStr != null) {
                    String[] split = rightStr.split(ConstantValue.COMMA_SYMBOL);
                    if (split.length == 1) {
                        return DataTypes.TIMESTAMP(Integer.parseInt(split[0].trim()));
                    }
                }
                return DataTypes.TIMESTAMP(6);
            case "DATE":
                return DataTypes.DATE();
            case "ARRAY":
            case "MAP":
            case "STRUCT":
            case "UNION":
            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
