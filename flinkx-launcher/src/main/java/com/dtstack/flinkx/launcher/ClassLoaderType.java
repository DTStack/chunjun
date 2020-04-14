package com.dtstack.flinkx.launcher;

/**
 * company: www.dtstack.com
 * author: toutian
 * create: 2019/10/17
 */
public enum ClassLoaderType {
    NONE, CHILD_FIRST, PARENT_FIRST, CHILD_FIRST_CACHE, PARENT_FIRST_CACHE;

    public static ClassLoaderType getByClassMode(String classMode) {
        ClassLoaderType classLoaderType;
        if ("classpath".equalsIgnoreCase(classMode)) {
            classLoaderType = ClassLoaderType.CHILD_FIRST;
        } else {
            classLoaderType = ClassLoaderType.PARENT_FIRST;
        }

        return classLoaderType;
    }
}
