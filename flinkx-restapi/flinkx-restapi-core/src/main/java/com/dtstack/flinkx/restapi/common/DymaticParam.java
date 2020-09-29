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

import java.util.List;
import java.util.Objects;

/**
 * DymaticParam
 *
 * @author by dujie@dtstack.com
 * @Date 2020/9/26
 */
public class DymaticParam extends ConstantParam implements ParamDefinitionNextAble ,lifecycle {

    private RestContext restContext;
    private ParamItemContext valueDymaticParam;
    private ParamItemContext nextvalueDymaticParam;

    public DymaticParam(String name, ParamType paramType, Class valueClass, String description, String format, RestContext context) {
        super(name, paramType, valueClass, null, description, format);
        this.restContext = context;
    }

    @Override
    public Object getValue() {
        Object data;
        if (restContext.getTime() > 0 && Objects.nonNull(nextvalueDymaticParam)) {
            data = nextvalueDymaticParam.getValue();
        } else {
            data = nextvalueDymaticParam.getValue();
        }
        data = format(data);
        return objectConvent(getValueType(), data);
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
        // restContext.calcute();
        return getValue();
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
      if(Objects.nonNull(valueDymaticParam)){
          valueDymaticParam.init();
      }

        if(Objects.nonNull(nextvalueDymaticParam)){
            nextvalueDymaticParam.init();
        }
    }

    public void setValueDymaticParam(ParamItemContext valueDymaticParam) {
        this.valueDymaticParam = valueDymaticParam;
    }

    public void setNextvalueDymaticParam(ParamItemContext nextvalueDymaticParam) {
        this.nextvalueDymaticParam = nextvalueDymaticParam;
    }
}
