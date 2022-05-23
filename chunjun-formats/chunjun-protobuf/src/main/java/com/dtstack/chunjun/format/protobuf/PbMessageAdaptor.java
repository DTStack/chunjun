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

package com.dtstack.chunjun.format.protobuf;

import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Descriptors;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

/** @author liuliu 2022/4/27 */
public class PbMessageAdaptor {
    protected final int[] normalToOneof;
    protected final int[] oneOfLastIndex;
    protected int size;

    public PbMessageAdaptor(
            List<Descriptors.FieldDescriptor> fieldDescriptorList,
            List<Descriptors.OneofDescriptor> oneofDescriptorList) {
        size = fieldDescriptorList.size();
        oneOfLastIndex = new int[oneofDescriptorList.size()];
        normalToOneof = new int[fieldDescriptorList.size()];
        Arrays.fill(normalToOneof, -1);
        for (int i = 0; i < oneofDescriptorList.size(); i++) {
            Descriptors.OneofDescriptor oneofDescriptor = oneofDescriptorList.get(i);
            for (Descriptors.FieldDescriptor fieldDescriptor : oneofDescriptor.getFields()) {
                normalToOneof[fieldDescriptor.getIndex()] = i;
                oneOfLastIndex[i] = fieldDescriptor.getIndex();
                size--;
            }
            size++;
        }
    }

    /** Check whether the protobuf field corresponding to index belongs to a oneof field */
    public boolean isOneOf(int index) {
        return normalToOneof[index] != -1;
    }

    /**
     * If the protobuf field corresponding to index belongs to a oneof field, then return the index
     * of the last field of the oneof
     */
    public Integer getLastOneOfIndex(int index) {
        int oneofIndex = normalToOneof[index];
        if (oneofIndex != -1) {
            return oneOfLastIndex[oneofIndex];
        }
        throw new ChunJunRuntimeException("");
    }

    public Method obtainMethod(String methodName, Class clazz) {
        try {
            Method method =
                    Arrays.stream(clazz.getDeclaredMethods())
                            .filter(m -> m.getName().equalsIgnoreCase(methodName))
                            .filter(
                                    m ->
                                            m.getParameterTypes().length == 0
                                                    || !AbstractMessage.Builder.class
                                                            .isAssignableFrom(
                                                                    m.getParameterTypes()[0]))
                            .toArray(Method[]::new)[0];
            method.setAccessible(true);
            return method;
        } catch (Exception e) {
            throw new ChunJunRuntimeException(
                    String.format(
                            "failed to obtain getMethod[%s] from class[%s]",
                            methodName, clazz.getName()));
        }
    }

    public int getSize() {
        return size;
    }
}
