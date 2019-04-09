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

package com.dtstack.flinkx.es.reader;

import com.dtstack.flinkx.es.EsUtil;
import com.dtstack.flinkx.inputformat.RichInputFormat;
import com.dtstack.flinkx.reader.ByteRateLimiter;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;
import org.elasticsearch.client.RestHighLevelClient;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * InputFormat for Elasticsearch
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class EsInputFormat extends RichInputFormat {

    protected String address;

    protected String index;

    protected String type;

    protected String query;

    protected List<String> columnValues;

    protected List<String> columnTypes;

    protected List<String> columnNames;

    protected int batchSize = 10;

    protected Map<String,Object> clientConfig;

    private int from;

    private int to;

    private int size;

    private int pos = 0;

    private transient RestHighLevelClient client;

    private List<Map<String, Object>> resultList;


    @Override
    public void configure(Configuration configuration) {
        client = EsUtil.getClient(address, clientConfig);
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
        return null;
    }

    @Override
    public InputSplit[] createInputSplits(int splitNum) throws IOException {
        long cnt = EsUtil.searchCount(client, index, type, query);
        if (cnt < splitNum) {
            EsInputSplit[] splits = new EsInputSplit[1];
            splits[0] = new EsInputSplit(0, (int)cnt);
            return splits;
        }

        List<EsInputSplit> splitList = new ArrayList<>();
        int size = (int) (cnt / splitNum);

        for(int i = 0; i < splitNum - 1; ++i) {
            splitList.add(new EsInputSplit(i*size, size));
        }

        int lastFrom = (splitNum - 1) * size;
        int lastSize = (int) (cnt - (splitNum - 1) * size);
        splitList.add(new EsInputSplit(lastFrom, lastSize));

        if(client != null) {
            client.close();
        }
        return splitList.toArray(new EsInputSplit[splitNum]);
    }


    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public void openInternal(InputSplit inputSplit) throws IOException {
        EsInputSplit esInputSplit = (EsInputSplit) inputSplit;
        from = esInputSplit.getFrom();
        size = esInputSplit.getSize();
        to = from + size;
        loadNextBatch();
    }

    private void loadNextBatch() {
        int range = batchSize;
        if (from + range > to) {
            range = to - from;
        }
        resultList = EsUtil.searchContent(client, index, type, query, from, range);
        from += range;
        pos = 0;
    }

    @Override
    public boolean reachedEnd() throws IOException {
        if(pos >= resultList.size()) {
            if(from >= to) {
                return true;
            }
            loadNextBatch();

            //check again
            return reachedEnd();

        }
        return false;
    }

    @Override
    public Row nextRecordInternal(Row row) throws IOException {
        Map<String,Object> thisRecord = resultList.get(pos);
        pos++;
        return EsUtil.jsonMapToRow(thisRecord, columnNames, columnTypes, columnValues);
    }

    @Override
    public void closeInternal() throws IOException {
        if(client != null) {
            client.close();
        }
    }

}
