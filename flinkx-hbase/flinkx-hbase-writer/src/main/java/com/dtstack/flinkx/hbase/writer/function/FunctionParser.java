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


package com.dtstack.flinkx.hbase.writer.function;


import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author jiangbo
 * @date 2019/7/24
 */
public class FunctionParser {

    private static final String COL_REGEX = "\\$\\([^\\(\\)]+?\\)";
    private static Pattern COL_PATTERN = Pattern.compile(COL_REGEX);

    private static final String FUNC_REGEX = "[^\\(\\)]+\\(.*\\)";
    private static Pattern FUNC_PATTERN = Pattern.compile(FUNC_REGEX);

    private static String LEFT_KUO = "(";
    private static String RIGHT_KUO = ")";
    private static String COLUMN_PREFIX = "$";
    private static String DELIM = "_";

    public static List<String> parseRowKeyCol(String express){
        List<String> columnNames = new ArrayList<>();
        Matcher matcher = COL_PATTERN.matcher(express);
        while (matcher.find()) {
            String colExpre = matcher.group();
            String col = colExpre.substring(colExpre.indexOf(LEFT_KUO)+1, colExpre.indexOf(RIGHT_KUO));
            columnNames.add(col);
        }

        return columnNames;
    }

    public static FunctionTree parse(String express){
        if(StringUtils.isEmpty(express)){
            throw new RuntimeException("Row key column express can not be null");
        }

        if(StringUtils.isEmpty(express.trim())){
            throw new RuntimeException("Row key column express can not be empty");
        }

        FunctionTree root = new FunctionTree();
        root.setFunction(new StringFunction());

        if(express.matches(FUNC_REGEX)){
            parseFunction(express, root);
        } else {
            parseColumn(express, root);
        }

        return root;
    }

    private static void parseColumn(String express, FunctionTree root){
        for (String split : express.split(DELIM)) {
            FunctionTree subFunc = new FunctionTree();
            if(split.startsWith(COLUMN_PREFIX)){
                String col = split.substring(split.indexOf(LEFT_KUO)+1, split.indexOf(RIGHT_KUO));

                subFunc.setFunction(new StringFunction());
                subFunc.setColumnName(col);
            } else {
                ConstantFunction function = new ConstantFunction();
                function.setValue(split);
                subFunc.setFunction(function);
            }

            root.addInputFunction(subFunc);
        }
    }

    private static void parseFunction(String express, FunctionTree root){
        Matcher matcher = FUNC_PATTERN.matcher(express);
        if(matcher.find()){
            String funcExpress = matcher.group();
            String funcName = funcExpress.substring(0, funcExpress.indexOf(LEFT_KUO));

            FunctionTree subFunc = new FunctionTree();
            subFunc.setFunction(FunctionFactory.createFuntion(funcName));

            funcExpress = funcExpress.substring(funcExpress.indexOf(LEFT_KUO)+1, funcExpress.lastIndexOf(RIGHT_KUO));
            parseColumn(funcExpress, subFunc);

            root.addInputFunction(subFunc);
        }
    }
}
