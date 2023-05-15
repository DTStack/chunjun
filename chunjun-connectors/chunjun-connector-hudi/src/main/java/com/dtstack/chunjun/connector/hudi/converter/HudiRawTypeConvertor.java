package com.dtstack.chunjun.connector.hudi.converter;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.api.java.typeutils.TypeExtractionException;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;

import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

public class HudiRawTypeConvertor {

    public static RowType generateFlinkRowType(List<FieldConfig> fieldList)
            throws TypeExtractionException {
        final LinkedList<RowType.RowField> rowFields = new LinkedList<>();
        for (FieldConfig fieldConf : fieldList) {
            rowFields.add(new RowType.RowField(fieldConf.getName(), typeConversion(fieldConf)));
        }
        return new RowType(false, rowFields);
    }

    /**
     * convert Chunjun Data Type to Hudi Type
     *
     * @param fieldConf
     * @return
     * @throws TypeExtractionException
     */
    private static LogicalType typeConversion(FieldConfig fieldConf)
            throws TypeExtractionException {
        final String originalType = fieldConf.getType().toLowerCase(Locale.ROOT);
        String type = getTypeWithNotLen(originalType);
        Boolean isNullable = !fieldConf.getNotNull();
        int l1, l2;
        switch (type) {
            case "smallint":
                return new SmallIntType(isNullable);
            case "tinyint":
                return new TinyIntType(isNullable);
            case "int":
                return new IntType(isNullable);
            case "bigint":
                return new BigIntType(isNullable);
            case "float":
                return new FloatType(isNullable);
            case "double":
                return new DoubleType(isNullable);
            case "decimal":
                l1 = getTypeLen(originalType, 0);
                l2 = getTypeLen(originalType, 1);
                return new DecimalType(isNullable, l1, l2);
            case "char":
                l1 = getTypeLen(originalType, 0);
                return new CharType(isNullable, l1);
            case "varchar":
                l1 = getTypeLen(originalType, 0);
                return new VarCharType(isNullable, l1);
            case "string":
                return new VarCharType(isNullable, VarCharType.MAX_LENGTH);
            case "boolean":
                return new BooleanType(isNullable);
            case "timestamp":
                l1 = getTypeLen(originalType, 0);
                return new TimestampType(isNullable, l1);
            case "date":
                return new DateType(isNullable);
            case "time":
                l1 = getTypeLen(originalType, 0);
                return new TimeType(isNullable, l1);
            case "binary":
                l1 = getTypeLen(originalType, 0);
                return new BinaryType(isNullable, l1);
            default:
                throw new TypeExtractionException(
                        "hudi async function does not support this data type currently: " + type);
        }
    }

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

    private static String getTypeWithNotLen(String typeWithLen) {
        int i = !typeWithLen.contains("(") ? typeWithLen.length() : typeWithLen.indexOf("(");
        return typeWithLen.substring(0, i);
    }

    private static int getTypeLen(String typeWithLe, int num) {
        String regExp = "[^0-9]+";
        Pattern r = Pattern.compile(regExp);
        final String[] lens = r.split(typeWithLe);
        if (lens.length > num + 1) {
            return Integer.parseInt(lens[num + 1]);
        }
        return -1;
    }
}
