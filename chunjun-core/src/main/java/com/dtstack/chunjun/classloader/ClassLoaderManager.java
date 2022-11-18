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

package com.dtstack.chunjun.classloader;

import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.ReflectionUtils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ClassLoaderManager {

    private static final Map<String, URLClassLoader> pluginClassLoader = new ConcurrentHashMap<>();

    public static <R> R newInstance(Set<URL> jarUrls, ClassLoaderSupplier<R> supplier)
            throws Exception {
        ClassLoader classLoader = retrieveClassLoad(new ArrayList<>(jarUrls));
        return ClassLoaderSupplierCallBack.callbackAndReset(supplier, classLoader);
    }

    public static <R> R newInstance(List<String> jarPathList, ClassLoaderSupplier<R> supplier)
            throws Exception {
        List<URL> jarUrlList = new ArrayList<>(jarPathList.size());
        for (String path : jarPathList) {
            jarUrlList.add(new File(path).toURI().toURL());
        }
        ClassLoader classLoader = retrieveClassLoad(jarUrlList);
        return ClassLoaderSupplierCallBack.callbackAndReset(supplier, classLoader);
    }

    private static URLClassLoader retrieveClassLoad(List<URL> jarUrls) {
        jarUrls.sort(Comparator.comparing(URL::toString));
        String jarUrlkey = StringUtils.join(jarUrls, "_");
        return pluginClassLoader.computeIfAbsent(
                jarUrlkey,
                k -> {
                    try {
                        URL[] urls = jarUrls.toArray(new URL[0]);
                        ClassLoader parentClassLoader =
                                Thread.currentThread().getContextClassLoader();
                        URLClassLoader classLoader = new URLClassLoader(urls, parentClassLoader);
                        log.info("jarUrl:{} create ClassLoad successful...", jarUrlkey);
                        return classLoader;
                    } catch (Throwable e) {
                        log.error(
                                "retrieve ClassLoad happens error:{}",
                                ExceptionUtil.getErrorMessage(e));
                        throw new RuntimeException("retrieve ClassLoad happens error");
                    }
                });
    }

    public static Set<URL> getClassPath() {
        Set<URL> classPaths = new HashSet<>();
        for (Map.Entry<String, URLClassLoader> entry : pluginClassLoader.entrySet()) {
            classPaths.addAll(Arrays.asList(entry.getValue().getURLs()));
        }
        return classPaths;
    }

    public static URLClassLoader loadExtraJar(List<URL> jarUrlList, URLClassLoader classLoader)
            throws IllegalAccessException, InvocationTargetException {
        for (URL url : jarUrlList) {
            if (url.toString().endsWith(".jar")) {
                urlClassLoaderAddUrl(classLoader, url);
            }
        }
        return classLoader;
    }

    private static void urlClassLoaderAddUrl(URLClassLoader classLoader, URL url)
            throws InvocationTargetException, IllegalAccessException {
        Method method = ReflectionUtils.getDeclaredMethod(classLoader, "addURL", URL.class);

        if (method == null) {
            throw new RuntimeException(
                    "can't not find declared method addURL, curr classLoader is "
                            + classLoader.getClass());
        }
        method.setAccessible(true);
        method.invoke(classLoader, url);
    }
}
