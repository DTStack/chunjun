package com.dtstack.flinkx.test.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 * @author jiangbo
 * @date 2020/3/6
 */
@Target(ElementType.TYPE)
public @interface TestCase {

    public Plugin plugin();

    public PluginType type();
}
