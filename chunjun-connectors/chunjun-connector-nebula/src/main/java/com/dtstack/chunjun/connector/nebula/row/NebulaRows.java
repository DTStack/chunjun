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

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.nebula.config.NebulaConfig;
import com.dtstack.chunjun.connector.nebula.utils.NebulaSchemaFamily;

import lombok.ToString;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.BATCH_INSERT_TEMPLATE;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.DELETE_EDGE_TEMPLATE;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.DELETE_VERTEX_TEMPLATE;

@ToString
public class NebulaRows implements Serializable {

    private static final long serialVersionUID = 1080560830091721157L;

    private final Boolean isVertex;

    private final List<NebulaVertex> nebulaVertices;

    private final List<NebulaEdge> nebulaEdges;

    private final NebulaConfig nebulaConfig;

    private List<String> values;

    public NebulaRows(NebulaConfig nebulaConfig) {
        this.nebulaVertices = new ArrayList<>();
        this.nebulaEdges = new ArrayList<>();
        this.values = new ArrayList<>();
        this.nebulaConfig = nebulaConfig;
        switch (nebulaConfig.getSchemaType()) {
            case TAG:
            case VERTEX:
                this.isVertex = true;
                break;
            default:
                this.isVertex = false;
        }
    }

    public List<String> getValues() {
        return values;
    }

    public void build() {
        List<String> var1 = this.values;
        if (isVertex) {
            addVertex(var1);
            this.values = new ArrayList<>();
            return;
        }
        addEdge(var1);
        this.values = new ArrayList<>();
    }

    public void addVertex(List<String> values) {
        nebulaVertices.add(new NebulaVertex(values, nebulaConfig));
    }

    public void addEdge(List<String> values) {
        nebulaEdges.add(new NebulaEdge(values, nebulaConfig));
    }

    public String getInsertStatement() {
        if (isVertex) {
            return String.format(
                            BATCH_INSERT_TEMPLATE,
                            NebulaSchemaFamily.VERTEX.getType(),
                            nebulaConfig.getEntityName(),
                            nebulaConfig.getFields().stream()
                                    .map(FieldConfig::getName)
                                    .collect(Collectors.joining(",")),
                            nebulaVertices.stream()
                                    .map(NebulaVertex::getInsertStatementString)
                                    .collect(Collectors.joining(",")))
                    + ";";
        }
        return String.format(
                        BATCH_INSERT_TEMPLATE,
                        NebulaSchemaFamily.EDGE.getType(),
                        nebulaConfig.getEntityName(),
                        nebulaConfig.getFields().stream()
                                .map(FieldConfig::getName)
                                .collect(Collectors.joining(",")),
                        nebulaEdges.stream()
                                .map(NebulaEdge::getInsertStatementString)
                                .collect(Collectors.joining(",")))
                + ";";
    }

    public String getUpdateStatement() {
        if (isVertex) {
            return nebulaVertices.stream()
                            .map(NebulaVertex::getUpdateStatementString)
                            .collect(Collectors.joining(";"))
                    + ";";
        }
        return nebulaEdges.stream()
                        .map(NebulaEdge::getUpdateStatementString)
                        .collect(Collectors.joining(";"))
                + ";";
    }

    public String getDeleteStatement() {
        if (isVertex) {
            return String.format(
                            DELETE_VERTEX_TEMPLATE,
                            nebulaVertices.stream()
                                    .map(NebulaVertex::getVid)
                                    .collect(Collectors.joining(",")))
                    + ";";
        }
        return String.format(
                        DELETE_EDGE_TEMPLATE,
                        nebulaConfig.getEntityName(),
                        nebulaEdges.stream()
                                .map(NebulaEdge::getDeleteStatementString)
                                .collect(Collectors.joining(",")))
                + ";";
    }

    public String getUpsertStatement() {
        if (isVertex) {
            return nebulaVertices.stream()
                            .map(NebulaVertex::getUpsertStatementString)
                            .collect(Collectors.joining(";"))
                    + ";";
        }
        return nebulaEdges.stream()
                        .map(NebulaEdge::getUpsertStatementString)
                        .collect(Collectors.joining(";"))
                + ";";
    }
}
