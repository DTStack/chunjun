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

import java.util.HashMap;
import java.util.Map;

/**
 * InnerVaribleFactory
 *
 * @author by dujie@dtstack.com
 * @Date 2020/9/26
 */
public class InnerVaribleFactory {

    private static Map<String, Paramitem> InnerVaribles;

    static {
        InnerVaribles = new HashMap<>(18);
        InnerVaribles.put("uuid", new UuidVarible());
        InnerVaribles.put("currenttime", new CurrentTimeVarible());

    }

    public static Paramitem createInnerVarible(String name) {
        return InnerVaribles.get(name);
    }

    public static boolean isInnerVariable(String name) {
        return InnerVaribles.containsKey(name);
    }


    public static void addVarible(String name,Paramitem paramitem) {
        InnerVaribles.put(name,paramitem);
    }

}
