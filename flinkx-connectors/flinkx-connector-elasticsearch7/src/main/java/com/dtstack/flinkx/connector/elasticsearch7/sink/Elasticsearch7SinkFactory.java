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

package com.dtstack.flinkx.connector.elasticsearch7.sink;

import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.connector.elasticsearch.ElasticsearchColumnConverter;
import com.dtstack.flinkx.connector.elasticsearch.ElasticsearchRawTypeMapper;
import com.dtstack.flinkx.connector.elasticsearch7.Elasticsearch7ClientFactory;
import com.dtstack.flinkx.connector.elasticsearch7.ElasticsearchConf;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.converter.RawTypeConverter;
import com.dtstack.flinkx.sink.SinkFactory;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.JsonUtil;
import com.dtstack.flinkx.util.PluginUtil;
import com.dtstack.flinkx.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import com.esotericsoftware.minlog.Log;
import org.apache.commons.lang.StringUtils;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @description:
 * @program: flinkx-all
 * @author: lany
 * @create: 2021/06/27 17:20
 */
public class Elasticsearch7SinkFactory extends SinkFactory {

    private final ElasticsearchConf elasticsearchConf;

    public Elasticsearch7SinkFactory(SyncConf syncConf) {
        super(syncConf);
        elasticsearchConf =
                JsonUtil.toObject(
                        JsonUtil.toJson(syncConf.getWriter().getParameter()),
                        ElasticsearchConf.class);
        elasticsearchConf.setColumn(syncConf.getWriter().getFieldList());
        super.initFlinkxCommonConf(elasticsearchConf);
        elasticsearchConf.setParallelism(1);
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        final RowType rowType =
                getFormatDescription(
                        TableUtil.createRowType(
                                elasticsearchConf.getColumn(), getRawTypeConverter()));
        TableSchema schema =
                TableUtil.createTableSchema(elasticsearchConf.getColumn(), getRawTypeConverter());
        ElasticsearchOutputFormatBuilder builder =
                new ElasticsearchOutputFormatBuilder(elasticsearchConf, schema);
        builder.setRowConverter(new ElasticsearchColumnConverter(rowType));
        return createOutput(dataSet, builder.finish());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return ElasticsearchRawTypeMapper::apply;
    }

    /**
     * 获取索引date列的format信息
     *
     * @param rowType
     * @return
     */
    public RowType getFormatDescription(RowType rowType) {
        RowType result = rowType;
        if (checkContainTimestamp(rowType)) {
            Map<String, Map<String, String>> indexColumnInfo = getIndexMappingProperties();
            if (indexColumnInfo != null) {
                List<RowType.RowField> rowFieldList = new ArrayList<>();
                for (RowType.RowField rowField : rowType.getFields()) {
                    Map<String, String> columnInfo = indexColumnInfo.get(rowField.getName());
                    String type = columnInfo.get("type");
                    if ("date".equalsIgnoreCase(type)
                            || rowField.getType().getTypeRoot()
                                    == LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE) {
                        String format = columnInfo.get("format");
                        if (StringUtils.isNotBlank(format)) {
                            format = format.split("\\|\\|")[0];
                        } else {
                            /**
                             * 默认格式
                             * https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html#strict-date-time
                             */
                            format = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
                        }
                        rowField =
                                new RowType.RowField(
                                        rowField.getName(), rowField.getType(), format);
                    }
                    rowFieldList.add(rowField);
                }
                result = new RowType(rowFieldList);
            }
        }
        return result;
    }

    /** @return 索引mapping配置详情 */
    public Map<String, Map<String, String>> getIndexMappingProperties() {
        Map<String, Map<String, String>> indexMappingProperties = new HashMap<>();
        RestHighLevelClient client = null;
        RestClient lowLevelClient = null;
        try {
            client =
                    Elasticsearch7ClientFactory.createClient(
                            elasticsearchConf,
                            PluginUtil.createDistributedCacheFromContextClassLoader());
            lowLevelClient = client.getLowLevelClient();
            String index = elasticsearchConf.getIndex();
            String endpoint = ConstantValue.SINGLE_SLASH_SYMBOL + index;
            Request request = new Request("GET", endpoint);
            Response response = lowLevelClient.performRequest(request);
            String resBody = EntityUtils.toString(response.getEntity());
            Map<String, Object> resBodyMap =
                    GsonUtil.GSON.fromJson(resBody, GsonUtil.gsonMapTypeToken);
            Map<String, Object> indexInfo = (Map<String, Object>) resBodyMap.get(index);
            Map<String, Object> indexMappingInfo = (Map<String, Object>) indexInfo.get("mappings");
            indexMappingProperties =
                    (Map<String, Map<String, String>>) indexMappingInfo.get("properties");
        } catch (Exception e) {
            Log.error("get index mapping properties false", e);
        } finally {
            try {
                if (lowLevelClient != null) {
                    lowLevelClient.close();
                }
                if (client != null) {
                    client.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return indexMappingProperties;
    }

    public boolean checkContainTimestamp(RowType rowType) {
        boolean flag = false;
        List<RowType.RowField> fields = rowType.getFields();
        for (RowType.RowField field : fields) {
            if (field.getType().getTypeRoot() == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
                flag = true;
                break;
            }
        }
        return flag;
    }
}
