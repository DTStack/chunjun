# 一、Introduce

The ElasticSearch Sink plugin supports writing data to the specified index. ​

# 二、Version support

Elasticsearch 7.x ​

# 三、Plugin name

| type|name|
| ---- | ----|
| Sync | elasticsearch7writer |
| SQL | elasticsearch7-x |

​

# 四、Param description

## 1、Sync

- hosts
    - Description：One or more Elasticsearch hosts to connect to。eg: ["localhost:9200"]
    - Required：required
    - Type：List<String>
    - Default：none
- index
    - Description：Elasticsearch index for every record.
    - Required：required
    - Type：String
    - Default：none
- username
    - Description：User name after basic authentication is enabled. Please notice that Elasticsearch doesn't pre-bundled security feature, but you can enable it by following the guideline to secure an Elasticsearch cluster.
    - Required：optional
    - Type：String
    - Default：none
- password
    - Description：Password used to connect to Elasticsearch instance. If username is configured, this option must be configured with non-empty string as well.
    - Required：optional
    - Type：String
    - Default：none
- batchSize
    - Description：Number of data pieces written in batches
    - Required：optional
    - Type：Integer
    - Default：1
- keyDelimiter
    - Description：Delimiter for composite keys ("_" by default), eg:“${col1}_${col2}”
    - Required：optional
    - Type：String
    - Default："_"
- column
    - Description：Columns to be synchronized
    - note：'*' is not supported.
    - format:
- connectTimeout
    - Description：Elasticsearch client max connect timeout.
    - Required：optional
    - Type：Integer
    - Default：5000
- socketTimeout
    - Description：Elasticsearch client max socket timeout.
    - Required：optional
    - Type：Integer
    - Default：1800000
- keepAliveTime
    - Description：Elasticsearch client connection max keepAlive time.
    - Required：optional
    - Type：Integer
    - Default：5000
- requestTimeout
    - Description：Elasticsearch client connection max request timeout.
    - Required：optional
    - Type：Integer
    - Default：2000
- maxConnPerRoute
    - Description：Elasticsearch client connection assigns maximum connection per route value.
    - Required：optional
    - Type：Integer
    - Default：10

```
"column": [{
    "name": "col", -- Column name, which can be found in a multi-level format
    "type": "string", -- Column type, when name is not specified, returns a constant column with the value specified by value
    "value": "value" -- Constant column value
}]
```

​

## 2、SQL

- hosts
    - Description：One or more Elasticsearch hosts to connect to。eg: "localhost:9200", Multiple addresses are delimited by semicolons.
    - Required：required
    - Type：List<String>
    - Default：none
- index
    - Description：Elasticsearch index for every record.
    - Required：required
    - Type：String
    - Default：none
- username
    - Description：User name after basic authentication is enabled. Please notice that Elasticsearch doesn't pre-bundled security feature, but you can enable it by following the guideline to secure an Elasticsearch cluster.
    - Required：optional
    - Type：String
    - Default：none
- password
    - Description：Password used to connect to Elasticsearch instance. If username is configured, this option must be configured with non-empty string as well.
    - Required：optional
    - Type：String
    - Default：无
- sink.bulk-flush.max-actions
    - Description：Maximum number of buffered actions per bulk request. Can be set to '0' to disable it.
    - Required：optional
    - Type：Integer
    - Default：1
- document-id.key-delimiter
    - Description：Delimiter for composite keys ("_" by default), eg:“${col1}_${col2}”
    - Required：optional
    - Type：String
    - Default："_"
- client.connect-timeout
    - Description：Elasticsearch client max connect timeout.
    - Required：optional
    - Type：Integer
    - Default：5000
- client.socket-timeout
    - Description：Elasticsearch client max socket timeout.
    - Required：optional
    - Type：Integer
    - Default：1800000
- client.keep-alive-time
    - Description：Elasticsearch client connection max keepAlive time.
    - Required：optional
    - Type：Integer
    - Default：5000
- client.request-timeout
    - Description：Elasticsearch client connection max request timeout.
    - Required：optional
    - Type：Integer
    - Default：2000
- client.max-connection-per-route
    - Description：Elasticsearch client connection assigns maximum connection per route value.
    - Required：optional
    - Type：Integer
    - Default：10

# 五、Data type

|supported | date type |
| --- | --- |
| yes |INTEGER,SMALLINT,DECIMAL,TIMESTAM DOUBLE,FLOAT,DATE,VARCHAR,VARCHAR,TIMESTAMP,TIME,BYTE|
| no | IP，binary, nested, object|

# 六、Sample demo

See the 'demo' folder in the 'FlinkX: Local: Test' module of the project.
