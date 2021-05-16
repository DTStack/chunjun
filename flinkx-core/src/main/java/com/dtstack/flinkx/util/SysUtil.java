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


import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;


/**
 * System Utilities
 * <p>
 * Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class SysUtil {
    protected static final Logger LOG = LoggerFactory.getLogger(SysUtil.class);

    public static void sleep(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<URL> findJarsInDir(File dir) throws MalformedURLException {
        List<URL> urlList = new ArrayList<>();

        if (dir.exists() && dir.isDirectory()) {
            File[] jarFiles = dir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.toLowerCase().endsWith(".jar") ||  name.toLowerCase().endsWith(".zip");
                }
            });

            for (File jarFile : jarFiles) {
                urlList.add(jarFile.toURI().toURL());
            }

        }

        return urlList;
    }

    public static String getCurrentPath() {
        Map<String, String> ENV = System.getenv();
        return ENV.get(ApplicationConstants.Environment.PWD.key());
    }

    public static List<String> unZip(String path,String savePath) throws IOException {

        ArrayList<String> jars = new ArrayList<>(32);
        ZipFile zf;
        zf = new ZipFile(path);
        Enumeration<ZipArchiveEntry> e = zf.getEntries();// 获得所有ZipEntry对象
        while (e.hasMoreElements()) {
            ZipArchiveEntry zn = e.nextElement();
            if (!zn.isDirectory()) {
                File newFile = new File(savePath + File.separator + zn.getName());
                LOG.info("start create file {}",newFile.getAbsolutePath());
                if (!newFile.createNewFile()) {
                    throw new IOException("create file" + newFile.getAbsolutePath() + " failed");
                }
                jars.add(newFile.getAbsolutePath());
                FileOutputStream output = new FileOutputStream(newFile);
                InputStream in = zf.getInputStream(zn);
                int read = 0;
                while ((read = in.read()) != -1) {
                    output.write(read);
                }
                output.close();
            }
        }
        return  jars;
    }


}


