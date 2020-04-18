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


package com.dtstack.flinkx.hdfs.reader;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;

import java.util.regex.Pattern;

/**
 * @author jiangbo
 * @date 2019/12/20
 */
public class HdfsPathFilter implements HdfsConfigurablePathFilter {

    private static Pattern PATTERN;

    private String regex;

    private String parentPath;

    private static final String DEFAULT_REGEX = ".*";

    public static final String KEY_REGEX = "file.path.regexFilter";
    public static final String KEY_PATH = "file.path";

    private static final PathFilter HIDDEN_FILE_FILTER = p -> {
        String name = p.getName();
        return !name.startsWith("_") && !name.startsWith(".");
    };

    public HdfsPathFilter() {
    }

    public HdfsPathFilter(String regex) {
        this.regex = regex;
        compileRegex();
    }

    private void compileRegex(){
        String compileRegex = StringUtils.isEmpty(regex) ? DEFAULT_REGEX : regex;
        PATTERN = Pattern.compile(compileRegex);
    }

    @Override
    public boolean accept(Path path) {
        if(!HIDDEN_FILE_FILTER.accept(path)){
            return false;
        }

        if (path.toUri().getPath().equals(parentPath)) {
            return true;
        }

        return PATTERN.matcher(path.getName()).matches();
    }

    @Override
    public void configure(JobConf jobConf) {
        this.regex = jobConf.get(KEY_REGEX);

        String path = jobConf.get(KEY_PATH);
        if (StringUtils.isNotEmpty(path)) {
            this.parentPath = new Path(path).toUri().getPath();
        }

        compileRegex();
    }
}

interface HdfsConfigurablePathFilter extends PathFilter, JobConfigurable{

}
