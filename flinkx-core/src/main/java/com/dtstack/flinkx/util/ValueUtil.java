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

package com.dtstack.flinkx.util;


import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Utilities for Type conversion
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class ValueUtil {

    public static Integer getInt(Object obj) {
        if(obj == null) {
            return null;
        } else if(obj instanceof String) {
            return Integer.valueOf((String) obj);
        } else {
            try {
                Method method = obj.getClass().getMethod("intValue");
                return (int) method.invoke(obj);
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                throw new RuntimeException("Unable to convert " + obj + " into Interger",e);
            }
        }
    }

}
