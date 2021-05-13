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
        ArrayList<String> imports = new ArrayList<>(32);
        imports.add("com.dtstack.flinkx.util.ClassUtil");
        imports.add("java.sql.Connection");
        imports.add("java.sql.DriverManager");
        imports.add("java.sql.SQLException");

        return getHelper(parentClassLoader, imports, IOracle9Helper.CLASS_READER_STR);
    }

    public static IOracle9Helper getOracleHelperOfWrite(ClassLoader parentClassLoader) throws IOException, CompileException {
        ArrayList<String> imports = new ArrayList<>(32);

        imports.add("com.dtstack.flinkx.rdb.util.DbUtil");
        imports.add("java.sql.Connection");
        imports.add("java.sql.SQLException");
        return getHelper(parentClassLoader, imports, IOracle9Helper.CLASS_WRITER_STR);

    }

    public static IOracle9Helper getHelper(ClassLoader parentClassLoader, List<String> imports, String javaContent) throws IOException, CompileException {
        ClassBodyEvaluator cbe = new ClassBodyEvaluator();
        cbe.setParentClassLoader(parentClassLoader);

        cbe.setDefaultImports(StringUtils.join(imports, ConstantValue.COMMA_SYMBOL));
        cbe.setImplementedInterfaces(new Class[]{IOracle9Helper.class});
        StringReader sr = new StringReader(javaContent);
        return (IOracle9Helper) cbe.createInstance(sr);
    }
}
