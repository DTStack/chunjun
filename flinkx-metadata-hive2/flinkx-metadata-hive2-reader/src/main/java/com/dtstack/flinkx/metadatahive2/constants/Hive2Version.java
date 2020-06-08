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

package com.dtstack.flinkx.metadatahive2.constants;


import java.io.Serializable;

import static com.dtstack.flinkx.metadata.MetaDataCons.*;

/**
 * Date: 2020/05/26
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class Hive2Version implements Serializable {

    private static final long serialVersionUID = -6326176329489914836L;

    protected String version;
    protected String source;

    public Hive2Version(String version, String source){
        this.version = version;
        this.source = source;
    }

    public int tablePosition(){
        int res = 0;
        if(Source.HIVE_SERVER2.getType().equals(source)){
            res = 1;
        }else if(Source.SPARK_THRIFT_SERVER.getType().equals(source)){
            res = 2;
        }
        return res;
    }

    public String[] tableParaFirstPos(){
        String[] res = new String[2];
        if(Source.HIVE_SERVER2.getType().equals(source)){
            res[0] = KEY_COLUMN_DATA_TYPE;
            res[1] = KEY_COLUMN_COMMENT;
        }else if(Source.SPARK_THRIFT_SERVER.getType().equals(source)){
            res[0] = KEY_COL_NAME;
            res[1] = KEY_COLUMN_DATA_TYPE;
        }
        return res;
    }

    public enum Source{
        //hive解析使用的server
        HIVE_SERVER2("hive server2"),SPARK_THRIFT_SERVER("spark thrift server");
        private String type;
        Source(String type){
            this.type = type;
        }
        public String getType(){
            return type;
        }
    }

    public enum Version {
        //hive对应的安装版本
        APACHE1("apache1"), APACHE2("apache2"), CDH1("cdh1"), CDH2("cdh2");

        private String type;

        Version(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }
    }

}
