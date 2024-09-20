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

package com.dtstack.chunjun.format.protobuf.util;

import com.dtstack.chunjun.format.protobuf.PbFormatType;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Descriptors;

import java.lang.reflect.Method;
import java.util.Arrays;

/** @author liuliu 2022/4/26 */
public abstract class PbReflectUtil {

    public static Tuple2<Descriptors.Descriptor, Class<? extends AbstractMessage>>
            getDescriptorByMessageClassName(String protoMessageClassName) {
        Class<? extends AbstractMessage> clazz;
        try {
            clazz = getClassByClassName(protoMessageClassName);
            return Tuple2.of(getDescriptorByClass(clazz), clazz);
        } catch (Exception e) {
            throw new ChunJunRuntimeException("failed to get proto Descriptor", e);
        }
    }

    public static Class getClassByClassName(String protoMessageClassName) {
        try {
            return Class.forName(protoMessageClassName);
        } catch (Exception e) {
            throw new ChunJunRuntimeException(
                    String.format(
                            "failed to get proto class by className %s", protoMessageClassName),
                    e);
        }
    }

    public static Descriptors.Descriptor getDescriptorByClass(
            Class<? extends AbstractMessage> clazz) {
        try {
            Method getDescriptor = clazz.getMethod("getDescriptor");
            getDescriptor.setAccessible(true);
            return (Descriptors.Descriptor) getDescriptor.invoke(clazz);
        } catch (Exception e) {
            throw new ChunJunRuntimeException(
                    String.format(
                            "failed to get proto Descriptor,final class name=%s", clazz.getName()),
                    e);
        }
    }

    public static String getClassNameByFullName(
            String protoClassName, String protoPackageName, String fullName) {
        return protoClassName
                + fullName.substring(protoPackageName.length()).replaceAll("\\.", "\\$");
    }

    /** @param suffix example as $Builder */
    public static Class getClassByFieldDescriptor(
            String protoClassName,
            String protoPackageName,
            Descriptors.FieldDescriptor fieldDescriptor,
            String suffix) {
        String className = null;
        try {
            className =
                    getClassNameByFullName(
                            protoClassName,
                            protoPackageName,
                            fieldDescriptor.getMessageType().getFullName());
            if (suffix != null) {
                int index =
                        className.lastIndexOf("$") == -1
                                ? className.lastIndexOf(".")
                                : className.lastIndexOf("$");
                className = className.substring(0, index) + suffix;
            }
            return Class.forName(className);
        } catch (Exception e) {
            throw new ChunJunRuntimeException(
                    String.format(
                            "cannot found class by fieldDescriptor,messageFullName=%s,final className=%s}",
                            fieldDescriptor.getMessageType().getFullName(), className));
        }
    }

    public static Tuple4<PbFormatType, PbFormatType, Class, Class> getMapTypeTuple(
            String protoClassName,
            String protoPackageName,
            Descriptors.FieldDescriptor fieldDescriptor) {
        // get builder method
        Class builderClass =
                getClassByFieldDescriptor(
                        protoClassName, protoPackageName, fieldDescriptor, "$Builder");
        Method[] methods =
                Arrays.stream(builderClass.getMethods())
                        .filter(
                                method ->
                                        method.getName()
                                                .equalsIgnoreCase(
                                                        "put" + fieldDescriptor.getName()))
                        .toArray(Method[]::new);
        // get map key and value type by builder method
        assert methods.length == 1;
        Class keyClass = methods[0].getParameterTypes()[0];
        Class valueClass = methods[0].getParameterTypes()[1];
        PbFormatType keyProtoType = PbFormatType.getProtoTypeForMapKey(keyClass);
        PbFormatType valueProtoType = PbFormatType.getProtoTypeForMapValue(valueClass);
        return Tuple4.of(keyProtoType, valueProtoType, keyClass, valueClass);
    }
}
