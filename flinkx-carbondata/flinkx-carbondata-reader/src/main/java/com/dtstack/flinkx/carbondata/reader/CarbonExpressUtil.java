package com.dtstack.flinkx.carbondata.reader;


import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanEqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanExpression;
import org.apache.carbondata.core.scan.expression.conditional.LessThanEqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.LessThanExpression;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class CarbonExpressUtil {

    private static final String EQUAL = "=";

    private static final String MORE_THAN = ">";

    private static final String LESS_THAN = "<";

    private static final String NOT_LESS_THAN = ">=";

    private static final String NOT_MORE_THAN = "<=";

    private static final String[] ops = {EQUAL, MORE_THAN, LESS_THAN, NOT_LESS_THAN, NOT_MORE_THAN};

    private CarbonExpressUtil() {
        // hehe
    }

    public static Expression eval(String expr, List<String> columnNames, List<String> columnTypes) {
        String left = null;
        String right = null;
        int i = 0;
        for(; i < ops.length; ++i) {
            String op = ops[i];
            if(expr.contains(op)) {
                int pos = expr.indexOf(op);
                left = expr.substring(0, pos);
                right = expr.substring(pos + op.length());
            }
            break;
        }

        if(i == ops.length) {
            throw new RuntimeException("unsupported op");
        }

        String op = ops[i];

        if(StringUtils.isBlank(left) || StringUtils.isBlank(right)) {
            throw new RuntimeException("Illegal filter Expression");
        }
        left = left.trim();
        right = right.trim();

        int leftIndex = columnNames.indexOf(left);
        if(leftIndex == -1) {
            throw new RuntimeException("columns do not contain " + left);
        }

        String columnType = columnTypes.get(leftIndex);
        DataType dataType = getDataType(columnType);
        ColumnExpression columnExpression = new ColumnExpression(left, dataType);
        LiteralExpression literalExpression = new LiteralExpression(right, dataType);

        switch (op) {
            case EQUAL:
                return new EqualToExpression(columnExpression, literalExpression);
            case MORE_THAN:
                return new GreaterThanExpression(columnExpression, literalExpression);
            case LESS_THAN:
                return new LessThanExpression(columnExpression, literalExpression);
            case NOT_LESS_THAN:
                return new GreaterThanEqualToExpression(columnExpression, literalExpression);
            case NOT_MORE_THAN:
                return new LessThanEqualToExpression(columnExpression, literalExpression);
        }

        return null;
    }



    private static DataType getDataType(String columnType) {
        columnType = columnType.toLowerCase();
        switch (columnType) {
            case "string":
                return DataTypes.STRING;
            case "varchar":
                return DataTypes.VARCHAR;
            case "int":
                return DataTypes.INT;
            case "short":
                return DataTypes.SHORT;
            case "date":
                return DataTypes.DATE;
            case "double":
                return DataTypes.DOUBLE;
            case "timestamp":
                return DataTypes.TIMESTAMP;
        }
        return null;
    }

    public static void main(String[] args) {
        String stmt = " a = 1 ";
        List<String> columnNames = new ArrayList<>();
        columnNames.add("b");
        columnNames.add("a");
        List<String> columnTypes = new ArrayList<>();
        columnTypes.add("string");
        columnTypes.add("int");
        Expression expr = eval(stmt, columnNames, columnTypes);
        System.out.println(expr);
    }

}
