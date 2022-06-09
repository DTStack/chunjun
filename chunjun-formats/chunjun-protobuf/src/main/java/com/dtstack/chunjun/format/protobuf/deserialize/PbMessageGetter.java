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

import com.dtstack.chunjun.format.protobuf.PbMessageAdaptor;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Descriptors;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

/**
 * Reflect all GET methods in the order of the fields in the protobuf message, then we can get the
 * values of all the fields in the Message object by index.
 *
 * @author liuliu 2022/4/13
 */
public class PbMessageGetter extends PbMessageAdaptor {

    private Method[] normalMethods;
    private Method[] oneofCaseMethods;
    private Method[] oneOfNumberMethods;

    public PbMessageGetter(
            List<Descriptors.FieldDescriptor> fieldDescriptorList,
            List<Descriptors.OneofDescriptor> oneofDescriptorList,
            Class<? extends AbstractMessage> clazz) {
        super(fieldDescriptorList, oneofDescriptorList);
        initMethods(fieldDescriptorList, oneofDescriptorList, clazz);
    }

    /**
     * Get the value in the protocolMessage object by fieldDescriptor index
     *
     * @param object protocolMessage
     * @param index index of fieldDescriptor
     */
    public Object getByIndex(Object object, int index) {
        try {
            return normalMethods[index].invoke(object);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new ChunJunRuntimeException(
                    String.format(
                            "failed to get filed from flink type[%s],index[%s]", object, index),
                    e);
        }
    }

    /**
     * If the index is a member of oneof,returns the corresponding index of the valid value in oneof
     *
     * @param object protocolMessage
     * @param index index of fieldDescriptor
     */
    public Integer getOneofCase(Object object, int index)
            throws InvocationTargetException, IllegalAccessException {
        int oneofIndex = normalToOneof[index];
        return (Integer)
                oneOfNumberMethods[oneofIndex].invoke(oneofCaseMethods[oneofIndex].invoke(object));
    }

    public void initMethods(
            List<Descriptors.FieldDescriptor> fieldDescriptorList,
            List<Descriptors.OneofDescriptor> oneofDescriptorList,
            Class<? extends AbstractMessage> clazz) {
        this.normalMethods =
                fieldDescriptorList.stream()
                        .map(fieldDescriptor -> obtainNormalGetMethod(fieldDescriptor, clazz))
                        .toArray(Method[]::new);
        this.oneofCaseMethods =
                oneofDescriptorList.stream()
                        .map(oneofDescriptor -> obtainOneofCaseMethod(oneofDescriptor, clazz))
                        .toArray(Method[]::new);
        this.oneOfNumberMethods =
                oneofDescriptorList.stream()
                        .map(oneofDescriptor -> obtainOneofNumberMethod(oneofDescriptor, clazz))
                        .toArray(Method[]::new);
    }

    public Method obtainOneofNumberMethod(
            Descriptors.OneofDescriptor oneofDescriptor, Class<? extends AbstractMessage> clazz) {
        Class caseClass;
        try {
            caseClass = Class.forName(clazz.getName() + "$" + oneofDescriptor.getName() + "Case");
        } catch (ClassNotFoundException e) {
            throw new ChunJunRuntimeException(
                    String.format(
                            "failed to get OneOfCase Class,caseName=%s",
                            oneofDescriptor.getName()));
        }
        return obtainMethod("getNumber", caseClass);
    }

    public Method obtainOneofCaseMethod(
            Descriptors.OneofDescriptor oneofDescriptor, Class<? extends AbstractMessage> clazz) {
        return obtainMethod("get" + oneofDescriptor.getName() + "Case", clazz);
    }

    /**
     * obtain get Method . methodName = get+descriptorName. if repeated,methodName =
     * get+descriptorName+List
     */
    public Method obtainNormalGetMethod(
            Descriptors.FieldDescriptor fieldDescriptor, Class<? extends AbstractMessage> clazz) {
        StringBuilder stringBuilder = new StringBuilder("get");
        stringBuilder.append(fieldDescriptor.getName());
        if (!fieldDescriptor.isMapField() && fieldDescriptor.isRepeated()) {
            stringBuilder.append("List");
        }
        return obtainMethod(stringBuilder.toString(), clazz);
    }
}
