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

package com.dtstack.flinkx.connector.jdbc.lookup.provider;

import com.alibaba.druid.pool.DruidDataSource;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.spi.DataSourceProvider;

import javax.sql.DataSource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * @program: flinkx
 * @author: wuren
 * @create: 2021/04/28
 */
public class DruidDataSourceProvider implements DataSourceProvider {

    private static final Logger LOG = LoggerFactory.getLogger(DruidDataSourceProvider.class);

    @Override
    public DataSource getDataSource(JsonObject config) {
        DruidDataSource ds = new DruidDataSource();

        Method[] methods = DruidDataSource.class.getMethods();
        Map<String, Method> methodMap = new HashMap<>();
        for (Method method : methods) {
            methodMap.put(method.getName(), method);
        }

        for (Map.Entry<String, Object> entry : config) {
            String name = entry.getKey();

            if ("provider_class".equals(name)) {
                continue;
            }

            String methodName = "set" + name.substring(0, 1).toUpperCase() + name.substring(1);

            try {
                Class paramClazz = entry.getValue().getClass();
                if (paramClazz.equals(Integer.class)) {
                    paramClazz = int.class;
                } else if (paramClazz.equals(Long.class)) {
                    paramClazz = long.class;
                } else if (paramClazz.equals(Boolean.class)) {
                    paramClazz = boolean.class;
                }
                Method method = DruidDataSource.class.getMethod(methodName, paramClazz);
                method.invoke(ds, entry.getValue());
            } catch (NoSuchMethodException e) {
                LOG.warn("no such method:" + methodName);
                LOG.warn(entry.getValue().getClass());
            } catch (InvocationTargetException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        return ds;
    }

    @Override
    public void close(DataSource dataSource) {
        if (dataSource instanceof DruidDataSource) {
            ((DruidDataSource) dataSource).close();
        }
    }

    @Override
    public int maximumPoolSize(DataSource dataSource, JsonObject config) {
        if (dataSource instanceof DruidDataSource) {
            return ((DruidDataSource) dataSource).getMaxActive();
        }
        return -1;
    }
}
