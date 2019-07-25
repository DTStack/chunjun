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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.hamcrest.MatcherAssert;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author jiangbo
 * @date 2019/7/24
 */
public class FunctionParser {

    private static final String FUNCTION_NAME_REGEX = "([a-z]|[A-Z]){1,}([a-z]|[A-Z]|[\\d])+";

    private static final String regex = "\\$\\([^\\)]+?\\)";

    private static Pattern pattern = Pattern.compile(regex);

    private static String LEFT_KUO = "(";
    private static String RIGHT_KUO = ")";
    private static String COLUMN_PREFIX = "$";
    private static String DELIM = "_";

    public static void main(String[] args) {
        String e2 = "$(cf:name5)_md5(md5($(cf:name1))_$(cf:name6)md5($(cf:name2)_test_$(cf:name3))_$(cf:name4))";

        String functionExpress = convertExpressToFunction(e2);
        System.out.println(functionExpress);

        parseFunc(functionExpress, 0);
    }

    /**
     * str(cf:name5) constant(_) md5(md5(str(cf:name1))constant(_)str(cf:name6)md5(str(cf:name2)constant(_)constant(test)constant(_)str(cf:name3))constant(_)str(cf:name4))
     * str(cf:name5) constant(_) md5()
     */
    private static void parseFunc(String express, int grade){
        for (int i = 0; i < express.length(); i++) {
            if(express.charAt(i) == '('){
                String token = express.substring(0, i);
                System.out.println(grade + " ---- func ----- " + token);

                int rightKuoIndex = express.indexOf(")", i+1);
                int leftKuoIndex = express.indexOf("(", i+1);
                if(leftKuoIndex != 0 && leftKuoIndex < rightKuoIndex){
                    int endIndex = xxx(express);
                    parseFunc(express.substring(i, endIndex), grade+1);
                    i = endIndex;
                    return;
                } else {
                    String params = express.substring(i+1, rightKuoIndex);
                    System.out.println(grade + "-----  params ------ " + params);

                    parseFunc(express.substring(rightKuoIndex+1), grade);
                    return;
                }
            } else if(express.charAt(i) == ')'){
                parseFunc(express.substring(i), grade);
                return;
            }
        }
    }

    private static int xxx(String express){

        // md5( md5(str(cf:name1))constant(_)str(cf:name6)md5(str(cf:name2)constant(_)constant(test)constant(_)str(cf:name3))constant(_)str(cf:name4))

        int firstLeftKuo = express.indexOf("(");

        int leftKuoNum = 0;
        for (int i = firstLeftKuo; i < express.length(); i++) {
            if(express.charAt(i) == '('){
                leftKuoNum++;
            } else if(express.charAt(i) == ')'){
                if(leftKuoNum == 0){
                    return i;
                } else {
                    leftKuoNum--;
                }
            }

        }

        return 0;
    }

    private static void parseChar(Iterator<String> it){

    }

    private static String convertExpressToFunction(String express){
        Matcher matcher = pattern.matcher(express);
        while (matcher.find()) {
            String colExpress = matcher.group();
            String col = colExpress.replace("$(", "").replace(")", "");
            express = express.replace(colExpress, String.format("str(%s)", col));
        }

        List<String> parts = new ArrayList<>();
        for (String split : express.split("_")) {
            if(!split.contains("(") && !split.contains(")")){
                parts.add(String.format("constant(%s)", split));
            } else {
                parts.add(split);
            }
        }

        express = StringUtils.join(parts, "constant(_)");

        return express;
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

        List<String> chars = new ArrayList<>();
        for (char c : express.toCharArray()) {
            chars.add(String.valueOf(c));
        }

        Iterator<String> it = chars.iterator();
        parseChars(it, root);

        return root;
    }

    private static void parseChars(Iterator<String> it, FunctionTree root){
        while (it.hasNext()){
            String c = it.next();
            if(COLUMN_PREFIX.equals(c)){
                parseColumn(it, root);
            } else {
                parseFunction(it, c, root);
            }
        }
    }

    private static void parseColumn(Iterator<String> it, FunctionTree root){
        List<String> chars = new ArrayList<>();

        while (it.hasNext()) {
            String c = it.next();
            if(LEFT_KUO.equals(c)){
                continue;
            }

            if(RIGHT_KUO.equals(c)){
                break;
            }

            chars.add(c);
        }

        String columnName = StringUtils.join(chars, "");
        System.out.println("column:" + columnName);

        FunctionTree columnFunction = new FunctionTree();
        columnFunction.setFunction(new StringFunction());
        columnFunction.setColumnName(columnName);

        root.addInputFunction(columnFunction);
    }

    private static void parseFunction(Iterator<String> it, String firstChar, FunctionTree root){
        List<String> chars = new ArrayList<>();

        if(!DELIM.equals(firstChar) && !RIGHT_KUO.equals(firstChar)){
            chars.add(firstChar);
        }

        while (it.hasNext()) {
            String c = it.next();

            if(DELIM.equals(c)){
                continue;
            }

            if(COLUMN_PREFIX.equals(c)){
                parseColumn(it, root);
                return;
            }

            if(LEFT_KUO.equals(c) || RIGHT_KUO.equals(c)){
                break;
            }

            chars.add(c);
        }

        if(CollectionUtils.isNotEmpty(chars)){
            String functionName = StringUtils.join(chars, "");
            System.out.println("function:" + functionName);

            FunctionTree functionFunction = new FunctionTree();
            functionFunction.setFunction(FunctionFactory.createFuntion(functionName));

            root.addInputFunction(functionFunction);

            parseChars(it, functionFunction);
        }

        parseChars(it, root);
    }
}
