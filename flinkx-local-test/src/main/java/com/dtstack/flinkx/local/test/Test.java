package com.dtstack.flinkx.local.test;

import java.io.IOException;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class Test {

    public static void main(String[] args) throws IOException {
        JarFile jar = new JarFile("/Users/yunhua/IdeaProjects/flinkx/flinkxplugins/pgwal/flinkx-connector-pgwal-feat_1.12_pluginMerge.jar");
        Enumeration<JarEntry> entries = jar.entries();
        while (entries.hasMoreElements()) {
            JarEntry jarEntry = entries.nextElement();
            System.out.println(jarEntry.getName());
        }
    }
}
