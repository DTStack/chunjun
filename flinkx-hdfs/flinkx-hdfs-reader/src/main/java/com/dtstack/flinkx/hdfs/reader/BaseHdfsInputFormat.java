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

import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.FileSystemUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * The Hdfs Implementation of InputFormat
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public abstract class BaseHdfsInputFormat extends BaseRichInputFormat {

    private static final char PARTITION_SPLIT_CHAR = '=';

    protected Map<String,Object> hadoopConfig;

    protected List<MetaColumn> metaColumns;

    protected String inputPath;

    protected String defaultFs;

    protected String delimiter;

    protected transient RecordReader recordReader;

    /**
     * 目前只支持UTF-8
     */
    protected String charsetName = "UTF-8";

    protected transient JobConf conf;

    protected transient org.apache.hadoop.mapred.InputFormat inputFormat;

    protected Object key;

    protected Object value;

    protected String filterRegex;

    protected transient UserGroupInformation ugi;

    protected boolean openKerberos;

    protected String currentPartition;

    protected transient FileSystem fs;

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        conf = buildConfig();

        openKerberos = FileSystemUtil.isOpenKerberos(hadoopConfig);
        if (openKerberos) {
            ugi = FileSystemUtil.getUGI(hadoopConfig, defaultFs);
        }
    }

    protected JobConf buildConfig() {
        JobConf conf = FileSystemUtil.getJobConf(hadoopConfig, defaultFs);
        conf.set(HdfsPathFilter.KEY_REGEX, filterRegex);
        FileSystemUtil.setHadoopUserName(conf);
        return conf;
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !recordReader.next(key, value);
    }

    @Override
    public void closeInternal() throws IOException {
        if(recordReader != null) {
            recordReader.close();
        }
    }

    /**
     * 从hdfs路径中获取当前分区信息
     * @param path hdfs路径
     */
    public void findCurrentPartition(Path path){
        String ptPathName = path.getParent().getName();
        currentPartition = ptPathName.substring(ptPathName.lastIndexOf(PARTITION_SPLIT_CHAR) + 1);
        for (MetaColumn column : metaColumns) {
            if(column.getPart()){
                column.setValue(currentPartition);
                break;
            }
        }
    }

}
