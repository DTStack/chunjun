# 一、Introduce

The ElasticSearch Source plugin supports reading data from an existing ElasticSearch cluster in a specified index.

# 二、Version support 

Elasticsearch 6.x
​
# 三、Plugin name

| type|name|
| ---- | ----|
| Sync | esreader、essource |
| SQL | es-x |

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
- column
   - Description：Columns to be synchronized
   - note：'*' is not supported.
   - format: 
  
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
   - Description：One or more Elasticsearch hosts to connect to。eg: ["localhost:9200"]
   - Required：required
   - Type：List<String>
   - Default：none
- index
   - Description：Elasticsearch index for every record. 
   - Required：required
   - Type：String
   - Default：none
- type
  - Description：Elasticsearch document type.
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
- bulk-flush.max-actions
   - Description：Maximum number of actions to buffer for each bulk request.
   - Required：否
   - Type：Integer
   - Default：1000

​

# 五、Data type


|supported | date type |
| --- | --- |
| yes |INTEGER,SMALLINT,DECIMAL,TIMESTAM DOUBLE,FLOAT,DATE,VARCHAR,VARCHAR,TIMESTAMP,TIME,BYTE|
| no | IP，binary, nested, object|


# 六、Sample demo

See the 'demo' folder in the 'FlinkX: Local: Test' module of the project.
