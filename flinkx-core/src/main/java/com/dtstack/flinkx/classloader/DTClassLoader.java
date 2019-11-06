/**
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

package com.dtstack.flinkx.classloader;

import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLStreamHandlerFactory;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The classloader used by Main program to load plugins of Readers' and Writers'
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class DTClassLoader extends URLClassLoader {

    private static Logger LOG = LoggerFactory.getLogger(DTClassLoader.class);

    protected ClassLoader parent;

    private static final String CLASS_FILE_SUFFIX = ".class";

    public DTClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
        this.parent = parent;
    }

    public DTClassLoader(URL[] urls) {
        super(urls);
        this.parent = Thread.currentThread().getContextClassLoader();
        LOG.info("urls=" + Arrays.asList(urls));
    }

    public DTClassLoader(URL[] urls, ClassLoader parent, URLStreamHandlerFactory factory) {
        super(urls, parent, factory);
        this.parent = Thread.currentThread().getContextClassLoader();
    }

    public DTClassLoader() {
        this(new URL[0]);
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        return this.loadClass(name, false);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            Class<?> clazz = null;

            // Search local repositories
            try {
                clazz = findClass(name);
                if (clazz != null) {
                    if (resolve){
                        resolveClass(clazz);
                    }
                    return (clazz);
                }
            } catch (ClassNotFoundException e) {
                // Ignore
            }

            // Delegate to parent unconditionally
            try {
                clazz = Class.forName(name, false, parent);
                if (clazz != null) {
                    if (resolve){
                        resolveClass(clazz);
                    }
                    return (clazz);
                }
            } catch (ClassNotFoundException e) {
                // Ignore
            }
        }

        throw new ClassNotFoundException(name);
    }

}
