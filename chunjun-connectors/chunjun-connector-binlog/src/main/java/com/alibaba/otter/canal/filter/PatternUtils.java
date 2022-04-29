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
package com.alibaba.otter.canal.filter;

import com.alibaba.otter.canal.filter.exception.CanalFilterException;
import com.google.common.base.Function;
import com.google.common.collect.MapMaker;
import com.google.common.collect.MapMakerHelper;
import com.google.common.collect.MigrateMap;
import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.PatternCompiler;
import org.apache.oro.text.regex.Perl5Compiler;

import java.util.Map;

/**
 * 提供{@linkplain Pattern}的lazy get处理
 *
 * @author jianghang 2013-1-22 下午09:36:44
 * @version 1.0.0
 */
public class PatternUtils {

    @SuppressWarnings("deprecation")
    private static Map<String, Pattern> patterns =
            MigrateMap.makeComputingMap(
                    MapMakerHelper.softValues(new MapMaker()),
                    new Function<String, Pattern>() {

                        @Override
                        public Pattern apply(String pattern) {
                            try {
                                PatternCompiler pc = new Perl5Compiler();
                                return pc.compile(
                                        pattern,
                                        Perl5Compiler.CASE_INSENSITIVE_MASK
                                                | Perl5Compiler.READ_ONLY_MASK
                                                | Perl5Compiler.SINGLELINE_MASK);
                            } catch (MalformedPatternException e) {
                                throw new CanalFilterException(e);
                            }
                        }
                    });

    public static Pattern getPattern(String pattern) {
        return patterns.get(pattern);
    }

    public static void clear() {
        patterns.clear();
    }
}
