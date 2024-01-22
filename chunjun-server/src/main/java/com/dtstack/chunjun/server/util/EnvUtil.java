package com.dtstack.chunjun.server.util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;

/**
 * 设置环境变量工具类 Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2023-07-28
 */
public class EnvUtil {
    public static void setEnv(Map<String, String> newenv) throws Exception {
        getModifiableEnvironment().putAll(newenv);
    }

    private static Map<String, String> getModifiableEnvironment() throws Exception {
        Class<?> pe = Class.forName("java.lang.ProcessEnvironment");
        Method getenv = pe.getDeclaredMethod("getenv");
        getenv.setAccessible(true);
        Object unmodifiableEnvironment = getenv.invoke(null);
        Class<?> map = Class.forName("java.util.Collections$UnmodifiableMap");
        Field m = map.getDeclaredField("m");
        m.setAccessible(true);
        return (Map<String, String>) m.get(unmodifiableEnvironment);
    }
}
