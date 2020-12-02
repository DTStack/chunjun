package com.dtstack.flinkx.metadataes6.constants;

public class MetaDataEs6Cons {

    public static final String KEY_INDICES = "indices";

    public static final String KEY_ADDRESS = "address";

    public static final String KEY_USERNAME = "username";

    public static final String KEY_PASSWORD = "password";

    public static final String KEY_TIMEOUT = "timeout";

    public static final String KEY_PATH_PREFIX = "pathPrefix";

    public static final String KEY_INDEX_HEALTH = "health";                     //green为正常，yellow表示索引不可靠（单节点），red索引不可用

    public static final String KEY_INDEX_STATUS = "indexStatus";                //表明索引是否打开

    public static final String KEY_INDEX_NAME = "indexName";

    public static final String KEY_INDEX_UUID = "indexUuid";                    //索引的唯一标识

    public static final String KEY_INDEX_PRI = "indexPri";                      //集群的主分片数

    public static final String KEY_INDEX_REP = "indexRep";

    public static final String KEY_INDEX_DOCS_COUNT = "indexDocsCount";         //文档数

    public static final String KEY_INDEX_DOCS_DELETED = "indexDocsDeleted";     //已删除文档数

    public static final String KEY_INDEX_SIZE = "indexSize";                    //索引存储的总容量

    public static final String KEY_INDEX_PRI_SIZE = "indexPriSize";             //主分片的总容量



}
