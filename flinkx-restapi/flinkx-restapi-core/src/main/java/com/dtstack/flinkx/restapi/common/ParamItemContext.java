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
package com.dtstack.flinkx.restapi.common;

import com.google.common.collect.Sets;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Stack;
import java.util.regex.Pattern;

/**
 * ParamItemContext
 *
 * @author by dujie@dtstack.com
 * @Date 2020/9/27
 */
public class ParamItemContext implements lifecycle {
    private List<Paramitem> valueitemList;
    private ParamDefinition paramDefinition;
    private boolean valueitemListIsNumber = false;
    private RestContext restContext;
    public static Pattern isNumber = Pattern.compile("[0-9+-//(//)///]+$");

    private HashSet set = Sets.newHashSet("+", "-", "*", "/");
    private Stack<Paramitem> opStack = new Stack<Paramitem>();
    private Stack<Paramitem> outStack = new Stack<Paramitem>();

    public ParamItemContext(List<Paramitem> valueitemList, ParamDefinition paramDefinition, RestContext restContext) {
        this.valueitemList = valueitemList;
        this.paramDefinition = paramDefinition;
        this.restContext = restContext;
    }


    public Object getValue() {

        if (valueitemListIsNumber) {
            //后缀表达式计算值
            return calculateSuffix(outStack);

        } else {
            StringBuilder stringBuilder = new StringBuilder();
            for (Paramitem paramitem : valueitemList) {
                stringBuilder.append(paramitem.getValue().toString());
            }
            return stringBuilder.toString();
        }
    }

    private boolean checkIsRight(List<Paramitem> actualItemList) {
        //2.检验中缀表达式是否合法
        //2.1操作符"+"，"-"，"*"，"/"不能出现在首位，末位
        if (set.contains(actualItemList.get(0).getValue().toString()) || set.contains(actualItemList.get(set.size() - 1).getValue().toString())) {
            return false;
        }
        //2.2操作符"+"，"-"，"*"，"/"“不能连续,括号成对匹配
        Stack<String> stack = new Stack<>();
        for (int i = 0; i < actualItemList.size(); i++) {
            if ("(".equals(actualItemList.get(i).getValue().toString())) {//左括号入栈
                stack.push(actualItemList.get(i).getValue().toString());
            } else if (")".equals(actualItemList.get(i).getValue().toString())) {//右括号出栈
                if (!("(".equals(stack.pop()))) {
                    return false;
                }
            } else if (("+".equals(actualItemList.get(i).getValue().toString()) || "-".equals(actualItemList.get(i).getValue().toString()) || "*".equals(actualItemList.get(i).getValue().toString()) || "/".equals(actualItemList.get(i).getValue().toString()))
                    && ("+".equals(actualItemList.get(i - 1).getValue().toString()) || "-".equals(actualItemList.get(i - 1).getValue().toString()) || "*".equals(actualItemList.get(i - 1).getValue().toString()) || "/".equals(actualItemList.get(i - 1).getValue().toString()))) {//连续操作符
                return false;
            }
        }
        return stack.empty();

    }

    public List<Paramitem> getParams() {
        return this.valueitemList;
    }


    @Override
    public void init() {
        //初始化
        //判断是否是符合数值表达式的 不是的话结束 判断dy返回值是数字类型还是含有format的string类型
        for (Paramitem item : valueitemList) {
            if (item instanceof ConstantVarible) {
                String value = item.getValue().toString();
                //正则校验
                if (!isNumber.matcher(value).find()) {
                    valueitemListIsNumber = false;
                    break;
                }
            } else {
                if (!(item.getValue() instanceof Date)) {
                    valueitemListIsNumber = false;
                    break;
                } else if (item.getValue() instanceof String) {
                    if (!NumberUtils.isNumber(item.getValue().toString())) {
                        if (item instanceof ReplaceParamItem) {
                            ReplaceParamItem item1 = (ReplaceParamItem) item;
                            if (item1.getReplaceParamDefinition(restContext) == null || StringUtils.isBlank(item1.getReplaceParamDefinition(restContext).getFormat())) {
                                valueitemListIsNumber = false;
                            }
                        }
                    }
                }
            }
        }

        //转为正则表达式 且转为后缀表达式
        if (valueitemListIsNumber) {
            List<Paramitem> actualItemList = new ArrayList<>(valueitemList.size() * 2);
            for (Paramitem item : valueitemList) {
                if (item instanceof ConstantVarible) {
                    String value = item.getValue().toString();
                    actualItemList.addAll(ParseUtil.parse(value, paramDefinition));
                } else {
                    actualItemList.add(item);
                }
            }
            //是否符合中缀表达式
            if (checkIsRight(actualItemList)) {
                valueitemList = actualItemList;
                doTrans();
            } else {
                //不符合 就是拼接了
                valueitemListIsNumber = false;
            }
        }
    }

    private void doTrans() { //其他类型自行转换
        for (int i = 0; i < valueitemList.size(); i++) {
            String ch = valueitemList.get(i).getValue().toString();
            switch (ch) {
                case "+":
                case "-":
                    operationOpStack(ch, valueitemList.get(i), 1);
                    break;
                case "*":
                case "/":
                    operationOpStack(ch, valueitemList.get(i), 2);
                    break;
                case "(":
                    opStack.push(valueitemList.get(i));
                    break;
                case ")":
                    operationParen();
                    break;
                default:
                    outStack.push(valueitemList.get(i));
                    break;
            }
        }
        while (!opStack.isEmpty()) {
            outStack.push(opStack.pop());
        }
    }

    public void operationOpStack(String opThis, Paramitem paramitem, int prec1) {//运算符栈操作
        while (!opStack.isEmpty()) {
            Paramitem opTop = opStack.pop();
            String value = opTop.getValue().toString();
            if (value == "(") {
                opStack.push(opTop);
            } else {
                int prec2;
                if (value == "+" || value == "-")
                    prec2 = 1;
                else
                    prec2 = 2;
                if (prec2 < prec1) {
                    opStack.push(opTop);
                    break;
                } else
                    outStack.push(opTop);
            }
        }
        opStack.push(paramitem);
    }

    public void operationParen() {
        while (!opStack.isEmpty()) {
            Paramitem c = opStack.pop();
            if (c.getValue() == "(")
                break;
            else
                outStack.push(c);
        }
    }


    public static BigDecimal calculateSuffix(Stack<Paramitem> outStack) {
        Stack<BigDecimal> stack = new Stack<>();
        while (!outStack.empty()) {
            Paramitem pop = outStack.pop();
            String value = pop.getValue().toString();
            if (("+".equals(value)) || ("-".equals(value)) || ("*".equals(value)) || ("/".equals(value))) {//操作符
                BigDecimal number2 = stack.pop();
                BigDecimal number1 = stack.pop();
                BigDecimal result = BigDecimal.ZERO;
                if ("+".equals(value)) {
                    result = number1.add(number2);
                } else if ("-".equals(value)) {
                    result = number1.subtract(number2);
                } else if ("*".equals(value)) {
                    result = number1.multiply(number2);
                } else {
                    result = number1.divide(number2);
                }
                stack.push(result);
            } else {//数字
                stack.push(new BigDecimal(value));
            }
        }
        return stack.pop();
    }
}