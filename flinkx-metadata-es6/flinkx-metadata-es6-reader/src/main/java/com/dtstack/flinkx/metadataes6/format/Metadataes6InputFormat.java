package com.dtstack.flinkx.metadataes6.format;

import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.metadataes6.constants.MetaDataEs6Cons;
import com.dtstack.flinkx.metadataes6.utils.Es6Util;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
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
    public InputSplit[] createInputSplitsInternal(int splitNum) {

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

    /**
     * 返回es集群下的所有索引
     * @return  索引列表
     * @throws IOException
     */
    protected List<Object> showIndices() throws IOException {

        List<Object> indiceName = new ArrayList<>();
        String[] indices = Es6Util.queryIndicesByCat(restClient);
        int n = 2;
        while (n < indices.length)
        {
            indiceName.add(indices[n]);
            n += 10;
        }

        return indiceName;

    }

    /**
     * 查询元数据
     * @param indexName   索引名称
     * @return  元数据
     * @throws IOException
     */
    protected Map<String, Object> queryMetaData(String indexName) throws IOException {

        Map<String, Object> result = new HashMap<>(16);
        Map<String, Object> indexProp = Es6Util.queryIndexProp(indexName,restClient);
        List<Map<String, Object>> alias = Es6Util.queryAliases(indexName,restClient);
        List<Map<String, Object>> column = Es6Util.queryColumns(indexName,restClient);
        result.put(MetaDataEs6Cons.KEY_INDEX,indexName);
        result.put(MetaDataEs6Cons.KEY_INDEX_PROP, indexProp);
        result.put(MetaDataEs6Cons.KEY_COLUMN,column);
        result.put(MetaDataEs6Cons.KEY_ALIAS,alias);

        return result;

    }

    @Override
    public boolean reachedEnd(){
        return !indexIterator.get().hasNext();
    }
}
