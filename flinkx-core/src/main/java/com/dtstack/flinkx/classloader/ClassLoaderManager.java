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

package com.dtstack.flinkx.classloader;

import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @company: www.dtstack.com
 * @author: toutian
 * @create: 2019/10/14
 */
public class ClassLoaderManager {

    private static final Logger LOG = LoggerFactory.getLogger(ClassLoaderManager.class);

    private static Map<String, URLClassLoader> pluginClassLoader = new ConcurrentHashMap<>();

    public static <R> R newInstance(Set<URL> jarUrls, ClassLoaderSupplier<R> supplier) throws Exception {
        ClassLoader classLoader = retrieveClassLoad(new ArrayList<>(jarUrls));
        return ClassLoaderSupplierCallBack.callbackAndReset(supplier, classLoader);
    }

    private static URLClassLoader retrieveClassLoad(List<URL> jarUrls) {
        jarUrls.sort(Comparator.comparing(URL::toString));
        String jarUrlkey = StringUtils.join(jarUrls, "_");
        return pluginClassLoader.computeIfAbsent(jarUrlkey, k -> {
            try {
                URL[] urls = jarUrls.toArray(new URL[0]);
                ClassLoader parentClassLoader = Thread.currentThread().getContextClassLoader();
                URLClassLoader classLoader = new URLClassLoader(urls, parentClassLoader);
                LOG.info("jarUrl:{} create ClassLoad successful...", jarUrlkey);
                return classLoader;
            } catch (Throwable e) {
                LOG.error("retrieve ClassLoad happens error:{}", ExceptionUtil.getErrorMessage(e));
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
}
