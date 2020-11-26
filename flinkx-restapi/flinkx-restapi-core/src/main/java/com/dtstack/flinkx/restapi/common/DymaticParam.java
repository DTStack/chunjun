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

import com.github.pfmiles.dropincc.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * DymaticParam
 *
 * @author by dujie@dtstack.com
 * @Date 2020/9/26
 */
public class DymaticParam extends ConstantParam implements ParamDefinitionNextAble {

    private RestContext restContext;

    private final List<Paramitem> dymaticNowString;
    private final List<Paramitem> dymaticNextString;


    private final String initValueExpression;
    private final String nextValueExpression;

    private Exe exe = getExe();


    public DymaticParam(String name, ParamType paramType, Class valueClass, String nowValue, String nextValue, String description, String format, RestContext context) {
        super(name, paramType, valueClass, null, description, format);
        // nowvalue一定是存在的
        initValueExpression = nowValue;

        dymaticNowString = ParamFactory.getVarible(nowValue);

        if (StringUtils.isBlank(nextValue)) {
            dymaticNextString = dymaticNowString;
            nextValueExpression = initValueExpression;
        } else {
            dymaticNextString = ParamFactory.getVarible(nextValue);
            nextValueExpression = nextValue;
        }
        this.restContext = context;
    }

    @Override
    public Object getValue() {
        return getDymaticValue(dymaticNowString, initValueExpression);
    }

    /**
     * 字符串解析为后缀表达式
     * `如何判断是动态变量
     * 如何判断是符合后缀表达式的
     * 后缀表达式计算
     *
     * @return
     */
    @Override
    public Object getNextValue() {
        return getDymaticValue(dymaticNextString, nextValueExpression);
    }

    //判断是否是运算符
    public boolean isOperator(String oper) {
        return oper.equals("+") || oper.equals("-") || oper.equals("*") || oper.equals("/");
    }

    //计算 如何变量1 2 只要有一个是字符串 那么就直接拼接 或者为null 为null 2+2+
    public int calculation(int num1, int num2, String oper) {
        switch (oper) {
            case "+":
                return num2 + num1;
            case "-":
                return num2 - num1;
            case "*":
                return num2 * num1;
            case "/":
                return num2 / num1;
            default:
                return 0;
        }
    }

    public int operationLv(char operation) {//给运算符设置优先级
        switch (operation) {
            case '+':
            case '-':
                return 1;
            case '*':
            case '/':
                return 2;
            case '(':
            case ')':
                return 3;
            default:
                return 0;
        }
    }


    @Override
    public void init() {

    }

    public Exe getExe() {
        Lang c = new Lang("Calculator");
        Grule expr = c.newGrule();
        c.defineGrule(expr, CC.EOF).action(new Action() {
            public BigDecimal act(Object matched) {
                return new BigDecimal(((Object[]) matched)[0].toString());
            }
        });
        TokenDef a = c.newToken("\\+");
        Grule addend = c.newGrule();
        expr.define(addend, CC.ks(a.or("\\-"), addend)).action(new Action() {
            public BigDecimal act(Object matched) {
                Object[] ms = (Object[]) matched;
                BigDecimal a0 = (BigDecimal) ms[0];
                Object[] aPairs = (Object[]) ms[1];
                for (Object p : aPairs) {
                    String op = (String) ((Object[]) p)[0];
                    BigDecimal a = (BigDecimal) ((Object[]) p)[1];
                    if ("+".equals(op)) {
                        a0 = a.add(a0);
                    } else {
                        a0 = a0.subtract(a);
                    }
                }
                return a0;
            }
        });
        TokenDef m = c.newToken("\\*");
        Grule factor = c.newGrule();
        addend.define(factor, CC.ks(m.or("/"), factor)).action(new Action() {
            public BigDecimal act(Object matched) {
                Object[] ms = (Object[]) matched;
                BigDecimal f0 = (BigDecimal) ms[0];
                Object[] fPairs = (Object[]) ms[1];
                for (Object p : fPairs) {
                    String op = (String) ((Object[]) p)[0];
                    BigDecimal f = (BigDecimal) ((Object[]) p)[1];
                    if ("*".equals(op)) {
                        f0 = f0.multiply(f);
                    } else {
                        f0 = f0.divide(f, BigDecimal.ROUND_HALF_UP, 3);
                    }
                }
                return f0;
            }
        });
        factor.define("\\(^[\\-]", expr, "\\)").action(new Action() {
            public BigDecimal act(Object matched) {
                return (BigDecimal) ((Object[]) matched)[1];
            }
        }).alt("\\(\\-\\d+(\\.\\d+)?\\)|\\d+(\\.\\d+)?").action(new Action() {
            public BigDecimal act(Object matched) {
               if( matched.toString().startsWith("(") && matched.toString().endsWith(")")){
                   return new BigDecimal(matched.toString().substring(1,matched.toString().length()-1));
               }else{
                   return new BigDecimal(matched.toString());
               }

            }
        });
        Exe exe = c.compile();
        return exe;
    }

    private Object getDymaticValue(List<Paramitem> dymaticString, String expression) {
        if (CollectionUtils.isEmpty(dymaticString)) {
            return expression;
        }
        Map<String, Object> tempReplaceValue = new HashMap<>(16);
        AtomicReference<String> tempExpression = new AtomicReference<>(expression);
        Object tempValue;
            dymaticString.forEach(k -> {
                Object value = k.getValue(restContext);
                tempReplaceValue.put(escapeExprSpecialWord("${" + k.getName() + "}"), value);
            });
            tempReplaceValue.forEach((k, v) -> {
                if(Objects.isNull(v)){
                    tempExpression.set(tempExpression.get().replaceFirst(k, 0+""));
                }else if (NumberUtils.isNumber(v.toString())) {
                    tempExpression.set(tempExpression.get().replaceFirst(k, v.toString()));
                } else if (v instanceof Date) {
                    tempExpression.set(tempExpression.get().replaceFirst(k, ((Date) v).getTime() + ""));
                }
                if (v instanceof java.sql.Date) {
                    tempExpression.set(tempExpression.get().replaceFirst(k, ((java.sql.Date) v).getTime() + ""));
                }
                if (v instanceof Timestamp) {
                    tempExpression.set(tempExpression.get().replaceFirst(k, ((Timestamp) v).getTime() + ""));
                }
            });


        try {
            tempValue = exe.eval(tempExpression.get());
        } catch (Exception e) {
            tempReplaceValue.forEach((k, v) -> {
                if(Objects.isNull(v)){
                    tempExpression.set(tempExpression.get().replaceFirst(k, ""));
                }else if (NumberUtils.isNumber(v.toString())) {
                    tempExpression.set(tempExpression.get().replaceFirst(k, v.toString()));
                } else if (v instanceof Date) {
                    tempExpression.set(tempExpression.get().replaceFirst(k, ((Date) v).toString() + ""));
                }
                if (v instanceof java.sql.Date) {
                    tempExpression.set(tempExpression.get().replaceFirst(k, ((java.sql.Date) v).toString() + ""));
                }
                if (v instanceof Timestamp) {
                    tempExpression.set(tempExpression.get().replaceFirst(k, ((Timestamp) v).toString() + ""));
                }
            });
            tempValue = initValueExpression;
        }
        restContext.updateValue(getType().name().toLowerCase(Locale.ENGLISH) + "." + getName(), tempValue);
        return tempValue;
    }
    /**
     * 转义正则特殊字符 （$()*+.[]?\^{},|）
     *
     * @param keyword
     * @return
     */
    public static String escapeExprSpecialWord(String keyword) {
        if (StringUtils.isNotBlank(keyword)) {
            String[] fbsArr = { "\\", "$", "(", ")", "*", "+", ".", "[", "]", "?", "^", "{", "}", "|" };
            for (String key : fbsArr) {
                if (keyword.contains(key)) {
                    keyword = keyword.replace(key, "\\" + key);
                }
            }
        }
        return keyword;
    }
}
