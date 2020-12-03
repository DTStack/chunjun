package com.dtstack.flinkx.metadataes6.constants;

public class MetaDataEs6Cons {

    public static final String KEY_INDICES = "indices";

    public static final String KEY_ADDRESS = "address";

    public static final String KEY_USERNAME = "username";

    public static final String KEY_PASSWORD = "password";

    public static final String KEY_TIMEOUT = "timeout";

    public static final String KEY_PATH_PREFIX = "pathPrefix";

    public static final String KEY_INDEX_HEALTH = "health";                     //green为正常，yellow表示索引不可靠（单节点），red索引不可用

    public static final String KEY_INDEX_STATUS = "status";                //表明索引是否打开

    public static final String KEY_INDEX = "index";

    public static final String KEY_INDEX_PROP = "indexProperties";

    public static final String KEY_INDEX_UUID = "uuid";                         //索引的唯一标识

    public static final String KEY_INDEX_PRI = "indexPri";                      //集群的主分片数

    public static final String KEY_INDEX_REP = "replicas";

    public static final String KEY_INDEX_DOCS_COUNT = "docs_count";         //文档数

    public static final String KEY_INDEX_DOCS_DELETED = "docs_deleted";     //已删除文档数

    public static final String KEY_INDEX_SIZE = "totalsize";                    //索引存储的总容量

    public static final String KEY_INDEX_PRI_SIZE = "pri_size";             //主分片的总容量

    public static final String KEY_INDEX_CREATE_TIME = "createtime";            //索引创建时间

    public static final String KEY_TYPE_NAME = "type";                          //索引下类型名

    public static final String KEY_INDEX_SHARDS = "shards";                 //分片数

    public static final String KEY_ALIAS = "alias";                             //索引别名

    public static final String KEY_COLUMN = "column";

    public static final String KEY_COLUMN_NAME = "column_name";                 //文档名

    public static final String KEY_DATA_TYPE = "data_type";                     //数据类型

    public static final String KEY_FIELDS = "fields";                           //字段映射

    public static final String KEY_FIELD_NAME = "field_name";                   //字段映射名

    public static final String KEY_FIELD_PROP = "field_prop";                   //字段映射参数

    public static final String API_METHOD_GET = "GET";                          //restAPI请求方式，GET
}
