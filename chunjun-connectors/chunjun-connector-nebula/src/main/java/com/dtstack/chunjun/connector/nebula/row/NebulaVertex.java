package com.dtstack.chunjun.connector.nebula.row;
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

import com.dtstack.chunjun.connector.nebula.conf.NebulaConf;
import com.dtstack.chunjun.connector.nebula.utils.NebulaSchemaFamily;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.UPDATE_VALUE_TEMPLATE;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.UPDATE_VERTEX_TEMPLATE;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.UPSERT_VALUE_TEMPLATE;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.UPSERT_VERTEX_TEMPLATE;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.VERTEX_VALUE_TEMPLATE;

/**
 * @author: gaoasi
 * @email: aschaser@163.com
 * @date: 2022/11/11 2:23 下午
 */
public class NebulaVertex implements Serializable {

    private String vid;

    private List<String> propValues;

    private NebulaConf nebulaConf;

    private List<String> propNames;

    public NebulaVertex(List<String> values, NebulaConf nebulaConf) {
        this.vid = values.get(0);
        this.propValues = values.subList(1, values.size());
        this.nebulaConf = nebulaConf;
        List<String> columnNames = nebulaConf.getColumnNames();
        List<String> val = columnNames.subList(1, columnNames.size());
        this.propNames = val;
    }

    public String getPropValuesString() {
        return String.join(",", propValues);
    }

    public String getVid() {
        return vid;
    }

    public void setVid(String vid) {
        this.vid = vid;
    }

    public List<String> getPropValues() {
        return propValues;
    }

    public void setPropValues(List<String> propValues) {
        this.propValues = propValues;
    }

    public String getInsertStatementString() {
        return String.format(VERTEX_VALUE_TEMPLATE, vid, getPropValuesString());
    }

    public String getUpdateStatementString() {
        List<String> updateProps = new ArrayList<>();
        for (int i = 0; i < propNames.size(); i++) {
            updateProps.add(
                    String.format(UPDATE_VALUE_TEMPLATE, propNames.get(i), propValues.get(i)));
        }
        String updatePropsString = String.join(",", updateProps);
        return String.format(
                UPDATE_VERTEX_TEMPLATE,
                NebulaSchemaFamily.VERTEX.getType(),
                nebulaConf.getEntityName(),
                vid,
                updatePropsString);
    }

    public String getUpsertStatementString() {
        List<String> updateProps = new ArrayList<>();
        for (int i = 0; i < propNames.size(); i++) {
            updateProps.add(
                    String.format(UPSERT_VALUE_TEMPLATE, propNames.get(i), propValues.get(i)));
        }
        String updatePropsString = String.join(",", updateProps);
        return String.format(
                UPSERT_VERTEX_TEMPLATE,
                NebulaSchemaFamily.VERTEX.getType(),
                nebulaConf.getEntityName(),
                vid,
                updatePropsString);
    }

    @Override
    public String toString() {
        return "NebulaVertex{" + "vid='" + vid + '\'' + ", propValues=" + propValues + '}';
    }
}
