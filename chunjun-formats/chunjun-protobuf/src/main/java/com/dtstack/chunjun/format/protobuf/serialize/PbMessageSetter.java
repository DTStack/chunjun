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

package com.dtstack.chunjun.format.protobuf.serialize;

import com.dtstack.chunjun.format.protobuf.PbMessageAdaptor;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Descriptors;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

/**
 * Reflect all Builder#Set methods in the order of the fields in the Protobuf Message, then we can
 * set the values by index.
 *
 * @author liuliu 2022/4/26
 */
public class PbMessageSetter extends PbMessageAdaptor {

    private AbstractMessage.Builder builder;
    private Method[] normalMethods;

    public PbMessageSetter(
            List<Descriptors.FieldDescriptor> fieldDescriptorList,
            List<Descriptors.OneofDescriptor> oneofDescriptorList,
            Class<? extends AbstractMessage> clazz) {
        super(fieldDescriptorList, oneofDescriptorList);
        initBuilder(clazz);
        initMethods(fieldDescriptorList, builder.getClass());
    }

    private void initBuilder(Class builderClass) {
        try {
            Method newBuilder = builderClass.getMethod("newBuilder");
            this.builder = (AbstractMessage.Builder) newBuilder.invoke(builderClass);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void setByIndex(Object o, int index) throws ChunJunRuntimeException {
        try {

            normalMethods[index].invoke(builder, o);
        } catch (InvocationTargetException
                | IllegalAccessException
                | IllegalArgumentException exception) {
            System.out.println(normalMethods[index].getName());
            Arrays.stream(normalMethods[index].getParameterTypes())
                    .forEach(p -> System.out.println(p.getName()));
            System.out.println(o);
            throw new ChunJunRuntimeException(
                    String.format(
                            "failed to set field by protobuf builder,builder class[%s],index[%s],filed[%s]",
                            builder.getClass().getName(), index, o),
                    exception);
        }
    }

    public Object build() {
        Object result = builder.build();
        builder.clear();
        return result;
    }

    private void initMethods(List<Descriptors.FieldDescriptor> fieldDescriptorList, Class clazz) {
        normalMethods =
                fieldDescriptorList.stream()
                        .map(
                                f -> {
                                    String action = "set";
                                    if (f.isMapField()) {
                                        action = "putAll";
                                    } else if (f.isRepeated()) {
                                        action = "addAll";
                                    }
                                    return action + f.getName();
                                })
                        .map(methodName -> obtainMethod(methodName, clazz))
                        .toArray(Method[]::new);
    }
}
