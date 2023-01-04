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

package com.dtstack.chunjun.connector.nebula.row;

import com.dtstack.chunjun.connector.nebula.config.NebulaConfig;
import com.dtstack.chunjun.connector.nebula.utils.NebulaSchemaFamily;

import lombok.ToString;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.UPDATE_VALUE_TEMPLATE;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.UPDATE_VERTEX_TEMPLATE;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.UPSERT_VALUE_TEMPLATE;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.UPSERT_VERTEX_TEMPLATE;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.VERTEX_VALUE_TEMPLATE;

@ToString
public class NebulaVertex implements Serializable {

    private static final long serialVersionUID = 1730140666287635863L;

    private String vid;

    private List<String> propValues;

    private final NebulaConfig nebulaConfig;

    private final List<String> propNames;

    public NebulaVertex(List<String> values, NebulaConfig nebulaConfig) {
        this.vid = values.get(0);
        this.propValues = values.subList(1, values.size());
        this.nebulaConfig = nebulaConfig;
        List<String> columnNames = nebulaConfig.getColumnNames();
        this.propNames = columnNames.subList(1, columnNames.size());
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
                nebulaConfig.getEntityName(),
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
                nebulaConfig.getEntityName(),
                vid,
                updatePropsString);
    }
}
