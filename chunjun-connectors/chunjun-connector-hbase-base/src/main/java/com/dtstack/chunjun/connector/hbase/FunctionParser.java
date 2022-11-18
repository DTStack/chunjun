/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.chunjun.connector.hbase;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FunctionParser {

    private static final String COL_REGEX = "\\$\\([^\\(\\)]+?\\)";
    private static final Pattern COL_PATTERN = Pattern.compile(COL_REGEX);

    private static final String LEFT_KUO = "(";
    private static final String RIGHT_KUO = ")";
    private static final String DELIM = "_";

    public static List<String> parseRowKeyCol(String express) {
        List<String> columnNames = new ArrayList<>();
        Matcher matcher = COL_PATTERN.matcher(express);
        while (matcher.find()) {
            String colExpre = matcher.group();
            String col =
                    colExpre.substring(colExpre.indexOf(LEFT_KUO) + 1, colExpre.indexOf(RIGHT_KUO));
            columnNames.add(col);
        }

        return columnNames;
    }

    public static FunctionTree parse(String express) {
        if (StringUtils.isEmpty(express) || StringUtils.isEmpty(express.trim())) {
            throw new RuntimeException("Row key column express can not be empty");
        }

        express = replaceColToStringFunc(express);

        FunctionTree root = new FunctionTree();
        root.setFunction(new StringFunction());

        if (express.startsWith(DELIM)) {
            FunctionTree child = new FunctionTree();
            child.setFunction(new ConstantFunction(""));
            root.addInputFunction(child);
            express = express.substring(1);
        }

        parseFunction(root, express);

        if (express.endsWith(DELIM)) {
            FunctionTree child = new FunctionTree();
            child.setFunction(new ConstantFunction(""));
            root.addInputFunction(child);
        }

        return root;
    }

    private static void parseFunction(FunctionTree root, String express) {
        int leftBracketsIndex = express.indexOf("(");
        if (leftBracketsIndex == -1) {
            root.setColumnName(express);
        } else {
            int rightBracketsIndex = findRightBrackets(leftBracketsIndex, express);
            if (rightBracketsIndex == -1) {
                throw new IllegalArgumentException("Illegal express:" + express);
            }

            String value = express.substring(0, leftBracketsIndex);
            if (StringUtils.isEmpty(value)) {
                throw new IllegalArgumentException(
                        "Parse function from express fail,function name can not be empty");
            }

            if (value.startsWith(DELIM)) {
                value = value.substring(1);
            }

            String[] splits = value.split(DELIM);
            for (int i = 0; i < splits.length - 1; i++) {
                FunctionTree child = new FunctionTree();
                child.setFunction(new ConstantFunction(splits[i]));
                root.addInputFunction(child);
            }

            FunctionTree child = new FunctionTree();
            child.setFunction(FunctionFactory.createFunction(splits[splits.length - 1]));
            root.addInputFunction(child);

            String subExpress = express.substring(leftBracketsIndex + 1, rightBracketsIndex);
            parseFunction(child, subExpress);

            String leftExpress = express.substring(rightBracketsIndex + 1);
            processLeftExpress(leftExpress, root);
        }
    }

    private static void processLeftExpress(String leftExpress, FunctionTree root) {
        if (StringUtils.isEmpty(leftExpress)) {
            return;
        }

        if (leftExpress.contains(LEFT_KUO)) {
            parseFunction(root, leftExpress);
        } else {
            if (leftExpress.startsWith(DELIM)) {
                leftExpress = leftExpress.substring(1);
            }

            if (StringUtils.isEmpty(leftExpress)) {
                return;
            }

            String[] splits = leftExpress.split(DELIM);
            for (String split : splits) {
                FunctionTree child = new FunctionTree();
                child.setFunction(new ConstantFunction(split));
                root.addInputFunction(child);
            }
        }
    }

    private static int findRightBrackets(int startIndex, String express) {
        boolean hasMeddleBrackets = false;
        for (int i = startIndex + 1; i < express.length(); i++) {
            char c = express.charAt(i);
            if ('(' == c) {
                hasMeddleBrackets = true;
            }

            if (')' == c) {
                if (hasMeddleBrackets) {
                    hasMeddleBrackets = false;
                } else {
                    return i;
                }
            }
        }

        return -1;
    }

    public static String replaceColToStringFunc(String express) {
        Matcher matcher = COL_PATTERN.matcher(express);
        while (matcher.find()) {
            String columnExpress = matcher.group();
            String column = columnExpress.substring(2, columnExpress.length() - 1);
            express = express.replace(columnExpress, String.format("string(%s)", column));
        }

        return express;
    }

    public static List<String> getRegexColumnName(String qualifier) {
        Matcher matcher = COL_PATTERN.matcher(qualifier);
        ArrayList<String> columnQualifier = new ArrayList<>();
        while (matcher.find()) {
            String columnGroup = matcher.group();
            String column = columnGroup.substring(2, columnGroup.length() - 1);
            columnQualifier.add(column);
        }
        return columnQualifier;
    }
}
