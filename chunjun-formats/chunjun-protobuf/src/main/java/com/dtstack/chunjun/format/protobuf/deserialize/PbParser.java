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

package com.dtstack.chunjun.format.protobuf.deserialize;

import com.dtstack.chunjun.format.protobuf.util.PbReflectUtil;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import java.lang.reflect.Method;

/**
 * trans byte[] to protobuf object by protobuf parseFrom method
 *
 * @author liuliu 2022/4/8
 */
public class PbParser {

    private Class messageClass;
    private Method parseMethod;

    public PbParser(String messageClassName) throws NoSuchMethodException {
        messageClass = PbReflectUtil.getClassByClassName(messageClassName);
        parseMethod = messageClass.getMethod("parseFrom", byte[].class);
        parseMethod.setAccessible(true);
    }

    public Object parse(byte[] bytes) {
        try {
            return parseMethod.invoke(messageClass, bytes);
        } catch (Exception e) {
            throw new ChunJunRuntimeException(
                    String.format(
                            "Failed to deserialize protocol data from byte[] to message object,messageClass=%s",
                            messageClass),
                    e);
        }
    }
}
