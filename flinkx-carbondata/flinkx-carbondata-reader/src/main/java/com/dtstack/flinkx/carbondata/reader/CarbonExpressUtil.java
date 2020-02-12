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


/**
 * Utilities for creating Expression Filter
 *
 * Company: www.dtstack.com
 * @author huyifan_zju@163.com
 */
public class CarbonExpressUtil {

    private static final String EQUAL = "=";

    private static final String MORE_THAN = ">";

    private static final String LESS_THAN = "<";

    private static final String NOT_LESS_THAN = ">=";

    private static final String NOT_MORE_THAN = "<=";

    private static final String[] OPERATIONS = {EQUAL, MORE_THAN, LESS_THAN, NOT_LESS_THAN, NOT_MORE_THAN};

    private CarbonExpressUtil() {
        // hehe
    }

    public static Expression eval(String expr, List<String> columnNames, List<DataType> columnTypes) {
        String left = null;
        String right = null;
        int i = 0;
        for(; i < OPERATIONS.length; ++i) {
            String op = OPERATIONS[i];
            if(expr.contains(op)) {
                int pos = expr.indexOf(op);
                left = expr.substring(0, pos);
                right = expr.substring(pos + op.length());
            }
            break;
        }

        if(i == OPERATIONS.length) {
            throw new RuntimeException("unsupported op");
        }

        String op = OPERATIONS[i];

        if(StringUtils.isBlank(left) || StringUtils.isBlank(right)) {
            throw new RuntimeException("Illegal filter Expression");
        }
        left = left.trim();
        right = right.trim();

        int leftIndex = columnNames.indexOf(left);
        if(leftIndex == -1) {
            throw new RuntimeException("columns do not contain " + left);
        }

        DataType dataType = columnTypes.get(leftIndex);
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
            default:
                return null;
        }
    }

}
