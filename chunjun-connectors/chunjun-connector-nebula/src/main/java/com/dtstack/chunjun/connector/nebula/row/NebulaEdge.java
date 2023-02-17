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

import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.EDGE_ENDPOINT_TEMPLATE;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.EDGE_ENDPOINT_WITHOUT_RANK_TEMPLATE;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.EDGE_VALUE_TEMPLATE;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.EDGE_VALUE_WITHOUT_RANKING_TEMPLATE;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.RANK;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.UPDATE_EDGE_TEMPLATE;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.UPDATE_EDGE_WITHOUT_RANK_TEMPLATE;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.UPDATE_VALUE_TEMPLATE;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.UPSERT_EDGE_TEMPLATE;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.UPSERT_EDGE_WITHOUT_RANK_TEMPLATE;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.UPSERT_VALUE_TEMPLATE;

@ToString
public class NebulaEdge implements Serializable {

    private static final long serialVersionUID = 8625111986484717125L;

    private final String srcId;

    private final String dstId;

    private Long rank;

    private Boolean rankExist;

    private final List<String> propValues;

    private final List<String> propNames;

    private final NebulaConfig nebulaConfig;

    public NebulaEdge(List<String> values, NebulaConfig nebulaConfig) {
        this.srcId = values.get(0);
        this.dstId = values.get(1);
        this.nebulaConfig = nebulaConfig;

        if (nebulaConfig.getColumnNames().contains(RANK)) {
            this.rank = Long.parseLong(values.get(2));
            rankExist = true;
            this.propNames = nebulaConfig.getColumnNames().subList(3, values.size());
            this.propValues = values.subList(3, values.size());
            return;
        }
        this.propNames = nebulaConfig.getColumnNames().subList(2, values.size());
        this.propValues = values.subList(2, values.size());
    }

    public String getInsertStatementString() {
        if (rankExist) {
            return String.format(
                    EDGE_VALUE_TEMPLATE, srcId, dstId, rank, String.join(",", propValues));
        }
        return String.format(
                EDGE_VALUE_WITHOUT_RANKING_TEMPLATE, srcId, dstId, String.join(",", propValues));
    }

    public String getUpdateStatementString() {
        List<String> updateProps = new ArrayList<>();
        for (int i = 0; i < propNames.size(); i++) {
            updateProps.add(
                    String.format(UPDATE_VALUE_TEMPLATE, propNames.get(i), propValues.get(i)));
        }
        String updatePropsString = String.join(",", updateProps);
        if (rankExist) {
            return String.format(
                    UPDATE_EDGE_TEMPLATE,
                    NebulaSchemaFamily.EDGE.getType(),
                    nebulaConfig.getEntityName(),
                    srcId,
                    dstId,
                    rank,
                    updatePropsString);
        }
        return String.format(
                UPDATE_EDGE_WITHOUT_RANK_TEMPLATE,
                NebulaSchemaFamily.EDGE.getType(),
                nebulaConfig.getEntityName(),
                srcId,
                dstId,
                updatePropsString);
    }

    public String getUpsertStatementString() {
        List<String> updateProps = new ArrayList<>();
        for (int i = 0; i < propNames.size(); i++) {
            updateProps.add(
                    String.format(UPSERT_VALUE_TEMPLATE, propNames.get(i), propValues.get(i)));
        }
        String updatePropsString = String.join(",", updateProps);
        if (rankExist) {
            return String.format(
                    UPSERT_EDGE_TEMPLATE,
                    NebulaSchemaFamily.EDGE.getType(),
                    nebulaConfig.getEntityName(),
                    srcId,
                    dstId,
                    rank,
                    updatePropsString);
        }
        return String.format(
                UPSERT_EDGE_WITHOUT_RANK_TEMPLATE,
                NebulaSchemaFamily.EDGE.getType(),
                nebulaConfig.getEntityName(),
                srcId,
                dstId,
                updatePropsString);
    }

    public String getDeleteStatementString() {
        if (rankExist) {
            return String.format(EDGE_ENDPOINT_TEMPLATE, srcId, dstId, rank);
        }
        return String.format(EDGE_ENDPOINT_WITHOUT_RANK_TEMPLATE, srcId, dstId);
    }
}
