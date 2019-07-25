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

package com.dtstack.flinkx.hbase.writer;

import com.dtstack.flinkx.hbase.writer.function.FunctionFactory;
import com.dtstack.flinkx.hbase.writer.function.IFunction;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//import org.apache.flink.types.Row;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

/**
 * @company: www.dtstack.com
 * @author: toutian
 * @create: 2019/7/23
 */
public class RowKeyFunctionTest {

    public static void main(String[] args) {
        HbaseOutputFormat format = new HbaseOutputFormat();
        format.columnNames = Lists.newArrayList("age", "name");
//        format.rowkeyColumnIndices = Lists.newArrayList(0);
//        format.rowkeyColumnValues = Lists.newArrayList("md5($(age)asdasd$(name))");
        new RowKeyFunction(format);
    }
}
