# 一、介绍

The ElasticSearch Lookup plug-in reads data from an existing ElasticSearch cluster in the specified index and associates it with the master table as a dimension table. Currently, the full dimension table and asynchronous dimension table are supported.

# 二、Version support

Elasticsearch 7.x ​

# 三、Plugin name

| type|name|
| ---- | ----|
| SQL | elasticsearch7-x |

​<br />

# 四、Param description

## 1、SQL

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
    - Default：none
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
- lookup.cache-type
    - Description：Dimension table type. Eg: ALL or LRU
    - Required：optional
    - Type：String
    - Default：LRU
- lookup.cache-period
    - Description：Full dimension table period time
    - Required：optional
    - Type：Long
    - Default：3600 * 1000L
- lookup.cache.max-rows
    - Description：Maximum number of entries in the dimension table cache.
    - Required：optional
    - Type：Long
    - Default：1000L
- lookup.cache.ttl
    - Description：Time To Live.
    - Required：optional
    - Type：Long
    - Default：60 * 1000L
- lookup.error-limit
    - Description：Number of non-compliant data in the dimension table.
    - Required：optional
    - Type：Long
    - Default：Long.MAX_VALUE
- lookup.fetch-size
    - Description：Fetch the number of items of dimension table data.
    - Required：optional
    - Type：Integer
    - Default：1000L
- lookup.parallelism
    - Description：Dimension table parallelism.
    - Required：optional
    - Type：Integer
    - Default：1

# 五、Data type

|supported | date type |
| --- | --- |
| yes |INTEGER,SMALLINT,DECIMAL,TIMESTAM DOUBLE,FLOAT,DATE,VARCHAR,VARCHAR,TIMESTAMP,TIME,BYTE|
| no | IP，binary, nested, object|

# 六、Sample demo

See the 'demo' folder in the 'FlinkX: Local: Test' module of the project.
