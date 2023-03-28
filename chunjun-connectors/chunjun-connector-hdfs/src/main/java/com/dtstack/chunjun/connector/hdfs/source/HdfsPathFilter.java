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
package com.dtstack.chunjun.connector.hdfs.source;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;

import java.util.regex.Pattern;

public class HdfsPathFilter implements PathFilter, JobConfigurable {

    public static final String KEY_REGEX = "file.path.regexFilter";
    private static final String DEFAULT_REGEX = ".*";
    private static final PathFilter HIDDEN_FILE_FILTER =
            p -> {
                String name = p.getName();
                return !name.startsWith("_") && !name.startsWith(".");
            };
    private static Pattern PATTERN;
    private String regex;

    public HdfsPathFilter() {}

    public HdfsPathFilter(String regex) {
        this.regex = regex;
        compileRegex();
    }

    @Override
    public boolean accept(Path path) {
        if (!HIDDEN_FILE_FILTER.accept(path)) {
            return false;
        }

        return PATTERN.matcher(path.getName()).matches();
    }

    @Override
    public void configure(JobConf jobConf) {
        this.regex = jobConf.get(KEY_REGEX);
        compileRegex();
    }

    /** compile regex */
    private void compileRegex() {
        String compileRegex = StringUtils.isEmpty(regex) ? DEFAULT_REGEX : regex;
        PATTERN = Pattern.compile(compileRegex);
    }
}
