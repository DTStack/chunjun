package com.dtstack.flinkx.metadataes6.format;

import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.metadataes6.utils.Es6Util;
import com.dtstack.flinkx.util.GsonUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.*;

public class Metadataes6InputFormat extends BaseRichInputFormat {

    protected String address;

    protected String username;

    protected String password;

    /**
     * 存放所有需要查询的index的名字
     */
    protected List<Object> indices;

    /**
     * 记录当前查询的表所在list中的位置
     */
    protected int start;

    protected Map<String,Object> clientConfig;

    private transient RestClient restClient;

    protected static transient ThreadLocal<Iterator<Object>> indexIterator = new ThreadLocal<>();

    @Override
    public void openInternal(InputSplit inputSplit) throws IOException {

        restClient = Es6Util.getClient(address, username, password, clientConfig);
        if (CollectionUtils.isEmpty(indices)) {
            indices = showIndices();
        }
        LOG.info("indicesSize = {}, indices = {}",indices.size(), indices);
        indexIterator.set(indices.iterator());

    }

    @Override
    public InputSplit[] createInputSplitsInternal(int splitNum) throws IOException {
        InputSplit[] splits = new InputSplit[splitNum];
        for (int i = 0; i < splitNum; i++) {
            splits[i] = new GenericInputSplit(i,splitNum);
        }

        return splits;
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        Map<String, Object> metaData = new HashMap<>(16);
        String indexName = (String) indexIterator.get().next();
        metaData.putAll(queryMetaData(indexName));
        LOG.info("query metadata: {}", metaData);
        return Row.of(metaData);
    }

    @Override
    protected void closeInternal() throws IOException {
        if(restClient != null) {
            restClient.close();
            restClient = null;
        }
    }


    protected List<Object> showIndices() throws IOException {
        List<Object> indices = new ArrayList<>();
        String method = "GET";
        String endpoint = "/_cat/indices";
        Response response =restClient.performRequest(method,endpoint);
        String resBody = EntityUtils.toString(response.getEntity());
        String [] arr = resBody.split("\\s+");
        int n = 2;
        while (n < arr.length)
        {
            indices.add(arr[n]);
            n += 10;
        }
        return indices;
    }

    protected Map<String, Object> queryMetaData(String indexName) throws IOException {
        Map<String, Object> result = new HashMap<>(16);
        Map<String, Object> indexProp = Es6Util.queryIndexProp(indexName,restClient);
        result.put("indexProp", indexProp);
        return result;
    }



    @Override
    public boolean reachedEnd() throws IOException {
        return !indexIterator.get().hasNext();
    }
}
