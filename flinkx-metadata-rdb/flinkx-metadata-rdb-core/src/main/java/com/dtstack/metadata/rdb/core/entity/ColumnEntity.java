/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.metadata.rdb.core.entity;

import java.io.Serializable;

/** 列层级的元数据
 * @author kunni@dtstack.com
 */

public class ColumnEntity implements Serializable {

    protected static final long serialVersionUID = 1L;

    /**是否是主键*/
    private String primaryKey;

    /**字段默认值*/
    protected String defaultValue;

    /**字段类型*/
    protected String type;

    /**字段名称*/
    protected String name;

    /**字段描述*/
    protected String comment;

    /**字段下标*/
    protected Integer index;

    /**字段是否可以为空*/
    protected String nullAble;

    /**字段长度*/
    protected Integer length;

    /**小数点长度*/
    protected Integer digital;

    public void setIndex(Integer index) {
        this.index = index;
    }

    public void setLength(Integer length) {
        this.length = length;
    }

    public void setDigital(Integer digital) {
        this.digital = digital;
    }

    public void setNullAble(String nullAble) {
        this.nullAble = nullAble;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getName() {
        return name;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    @Override
    public String toString() {
        return "ColumnEntity{" +
                "primaryKey='" + primaryKey + '\'' +
                ", defaultValue='" + defaultValue + '\'' +
                ", type='" + type + '\'' +
                ", name='" + name + '\'' +
                ", comment='" + comment + '\'' +
                ", index=" + index +
                ", nullAble='" + nullAble + '\'' +
                ", length=" + length +
                ", digital=" + digital +
                '}';
    }
}
