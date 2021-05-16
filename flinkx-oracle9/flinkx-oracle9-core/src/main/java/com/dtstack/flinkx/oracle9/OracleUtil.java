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

package com.dtstack.flinkx.oracle9;

import com.dtstack.flinkx.constants.ConstantValue;
import org.apache.commons.lang.StringUtils;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ClassBodyEvaluator;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class OracleUtil {
    public static IOracle9Helper getOracleHelperOfReader(ClassLoader parentClassLoader) throws IOException, CompileException {

        ClassBodyEvaluator cbe = new ClassBodyEvaluator();
        cbe.setParentClassLoader(parentClassLoader);

        cbe.setDefaultImports("com.dtstack.flinkx.util.ClassUtil","java.sql.Connection","java.sql.DriverManager","java.sql.SQLException","java.io.BufferedReader");
        cbe.setImplementedInterfaces(new Class[]{IOracle9Helper.class});
        StringReader sr = new StringReader(IOracle9Helper.CLASS_READER_STR);
        return (IOracle9Helper) cbe.createInstance(sr);
    }

    public static IOracle9Helper getOracleHelperOfWrite(ClassLoader parentClassLoader) throws IOException, CompileException {

        ClassBodyEvaluator cbe = new ClassBodyEvaluator();
        cbe.setParentClassLoader(parentClassLoader);

        cbe.setDefaultImports("com.dtstack.flinkx.rdb.util.DbUtil","java.sql.Connection","java.sql.SQLException","java.io.BufferedReader");
        cbe.setImplementedInterfaces(new Class[]{IOracle9Helper.class});
        StringReader sr = new StringReader(IOracle9Helper.CLASS_WRITER_STR);
        return (IOracle9Helper) cbe.createInstance(sr);

    }

}
