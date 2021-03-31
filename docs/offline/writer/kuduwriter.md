# Kudu Writer

## 一、插件名称
名称：**kuduwriter**

## 二、支持的数据源版本
**kudu 1.10及以上**


## 三、参数说明


- **masterAddresses**
  - 描述： master节点地址:端口，多个以,隔开
  - 必选：是
  - 参数类型：string
  - 默认值：无

<br/>

- **table**
  - 描述： kudu表名
  - 必选：是
  - 参数类型：string
  - 默认值：无

<br/>

- **column**
  - 描述：需要生成的字段
  - 格式
```json
"column": [{
    "name": "col",
    "type": "string"
}]
```

- 属性说明:
  - name：字段名称
  - type：字段类型
- 必选：是
- 参数类型：数组
- 默认值：无

<br/>

- **writeMode**
  - 描述： kudu数据写入模式：
    - 1、insert
    - 2、update
    - 3、upsert
  - 必选：是
  - 参数类型：string
  - 默认值：无

<br/>

- **flushMode**
  - 描述： kudu session刷新模式：
    - 1、auto_flush_sync 同步刷新
    - 2、auto_flush_background  后台自动刷新
    - 3、manual_flush 手动刷新
  - 必选：否
  - 参数类型：string
  - 默认值：auto_flush_sync

<br/>

- **batchInterval**
  - 描述： 单次批量写入数据条数
  - 必选：否
  - 参数类型：int
  - 默认值：1

<br/>

- **authentication**
  - 描述：认证方式，kudu开启kerberos时需要配置authentication为Kerberos
  - 必选：否
  - 参数类型：string
  - 默认值：无

<br/>

- **workerCount**
  - 描述：worker线程数
  - 必选：否
  - 字段类型：int
  - 默认值：默认为cpu核心数*2

<br/>

- **bossCount**
  - 描述：boss线程数
  - 必选：否
  - 字段类型：int
  - 默认值：1

<br/>

- **operationTimeout**
  - 描述：普通操作超时时间，单位毫秒
  - 必选：否
  - 字段类型：long
  - 默认值：30000

<br/>

- **adminOperationTimeout**
  - 描述： 管理员操作(建表，删表)超时时间，单位毫秒
  - 必选：否
  - 字段类型：long
  - 默认值：15000

<br/>

- **hadoopConfig**
  - 描述： kudu开启kerberos，需要配置kerberos相关参数
  - 必选：否
  - 字段类型：map
  - 默认值：无



## 四、配置示例
```json
{
  "job" : {
    "content" : [ {
      "reader": {
        "name": "streamreader",
        "parameter": {
          "column": [
            {
              "name": "id",
              "type": "string"
            }, {
              "name": "name",
              "type": "string"
            }, {
              "name": "age",
              "type": "int"
            }, {
              "name": "sex",
              "type": "int"
            }
          ],
          "sliceRecordCount" : [100]
        }
      },
      "writer" : {
        "parameter": {
          "column": [
            {
              "name": "id",
              "type": "string"
            }, {
              "name": "name",
              "type": "string"
            }, {
              "name": "age",
              "type": "int"
            }, {
              "name": "sex",
              "type": "int"
            }
          ],
          "masterAddresses": "host:7051",
          "table": "student",
          "writeMode": "insert",
          "flushMode": "manual_flush",
          "batchInterval": 10000,
          "workerCount": 2,
          "bossCount": 1
        },
        "name": "kuduwriter"
      }
    } ],
    "setting": {
      "speed": {
        "channel": 1
      },
      "restore": {
        "isRestore": false,
        "isStream" : false
      }
    }
  }
}
```
