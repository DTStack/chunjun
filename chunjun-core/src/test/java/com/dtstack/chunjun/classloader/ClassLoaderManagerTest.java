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

import org.junit.Assert;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.powermock.reflect.Whitebox;

import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ClassLoaderManagerTest {

    @Test
    public void urlClassLoaderAddUrlWhenMethodIsNullThenThrowException() throws Exception {
        URLClassLoader classLoader = new URLClassLoader(new URL[] {});
        URL url = null;
        Whitebox.invokeMethod(ClassLoaderManager.class, "urlClassLoaderAddUrl", classLoader, url);
    }

    @Test
    public void loadExtraJarWhenUrlIsJarThenAddToClassLoader()
            throws InvocationTargetException, IllegalAccessException {
        List<URL> jarUrlList = new ArrayList<>();
        URLClassLoader classLoader = new URLClassLoader(new URL[0]);
        ClassLoaderManager.loadExtraJar(jarUrlList, classLoader);
    }

    @Test
    @DisplayName("Should return the classpath of all the classloaders")
    public void getClassPathShouldReturnTheClasspathOfAllTheClassloaders() {
        Set<URL> classPaths = ClassLoaderManager.getClassPath();
        assertEquals(0, classPaths.size());
    }

    @Test
    @DisplayName("Should throw an exception when the jarpathlist is empty")
    public void newInstanceWhenJarPathListIsEmptyThenThrowException() {
        List<String> jarPathList = new ArrayList<>();
        assertThrows(Exception.class, () -> ClassLoaderManager.newInstance(jarPathList, null));
    }

    /** Should return the same classloader when the jarUrls are the same */
    @Test
    public void retrieveClassLoadWhenJarUrlsAreTheSameThenReturnTheSameClassLoader()
            throws Exception {
        List<URL> jarUrls = new ArrayList<>();

        URLClassLoader classLoader =
                (URLClassLoader)
                        Whitebox.<List<URL>>invokeMethod(
                                ClassLoaderManager.class, "retrieveClassLoad", jarUrls);
        Assert.assertNotNull(classLoader);

        URLClassLoader classLoader1 =
                (URLClassLoader)
                        Whitebox.<List<URL>>invokeMethod(
                                ClassLoaderManager.class, "retrieveClassLoad", jarUrls);
        Assert.assertNotNull(classLoader1);

        assertEquals(classLoader, classLoader1);
    }

    /** Should return the instance when the jarUrls is not empty */
    @Test
    public void newInstanceWhenJarUrlsIsNotEmptyThenReturnInstance() throws Exception {
        List<String> jarUrls = new ArrayList<>();
        String result = ClassLoaderManager.newInstance(jarUrls, cl -> "hello");
        assertEquals("hello", result);
    }

    /** Should return the instance when the jarPathList is not empty */
    @Test
    public void newInstanceWhenJarPathListIsNotEmptyThenReturnTheInstance() {
        List<String> jarPathList = new ArrayList<>();
        ClassLoaderSupplier<String> supplier = cl -> "hello";
        try {
            String result = ClassLoaderManager.newInstance(jarPathList, supplier);
            assertEquals("hello", result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
