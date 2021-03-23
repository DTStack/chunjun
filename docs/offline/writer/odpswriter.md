# ODPS Writer

## 一、插件名称
名称：**odpswriter**

## 二、参数说明


- **table**
  - 描述：读取数据表的表名称（大小写不敏感）
  - 必选：是
  - 字段类型：string
  - 默认值：无

<br/>

- **partition**
  - 描述：需要写入数据表的分区信息，必须指定到最后一级分区。把数据写入一个三级分区表，必须配置到最后一级分区，例如pt=20150101/type＝1/biz=2。
  - 注意：**如果是分区表，该选项必填，如果非分区表，该选项不可填写**
  - 必选：否
  - 字段类型：string
  - 默认值：无

<br/>

- **column**
  - 描述：需要读取的字段
  - 格式：
```json
"column": [{
    "name": "col",
    "type": "datetime"
}]
```

- 属性说明:
  - name：字段名称 必填
  - type：字段类型，可以和数据库里的字段类型不一样，程序会做一次类型转换 必填
- 必选：是
- 字段类型：数组
- 默认值：无

<br/>

- **writeMode**
  - 描述：写入模式，支持append和overwrite
  - 必填：否
  - 字段类型：string
  - 默认值：append

<br/>

- **bufferSize**
  - 描述：写入缓存大小，单位M，odps写入数据时会先缓存，达到一定值后才会写入数据，如果写入数据时出现内存溢出，可以降低此参数的值。
  - 必填：否
  - 字段类型：long
  - 默认值：64M

<br/>

- **odpsConfig**
  - 描述：ODPS的配置信息
  - 必选：是
  - 字段类型 map
  - 默认值：无
  - 可选配置：
    - **odpsServer**
      - 描述：odps服务地址
      - 必选：否
      - 字段类型 string
      - 默认值：[http://service.odps.aliyun.com/api](http://service.odps.aliyun.com/api)
    - **accessId**
      - 描述：ODPS系统登录ID
      - 必选：是
      - 字段类型 string
      - 默认值：无
    - **accessKey**
      - 描述：ODPS系统登录Key
      - 必选：是
      - 字段类型 string
      - 默认值：无
    - **project**
      - 描述：读取数据表所在的 ODPS 项目名称（大小写不敏感）
      - 必选：是
      - 字段类型 string
      - 默认值：无
    - **packageAuthorizedProject**
      - 描述：ODPS认证项目
      - 注意：当 **packageAuthorizedProject **不为空时，当前project取packageAuthorizedProject对应值 而不是 project 对应的值
      - 必选：否
      - 字段类型 string
      - 默认值：无
    - **accountType**
      - 描述：odps账户类型
      - 注意：目前只支持 aliyun 类型
      - 必选：否
      - 字段类型 string
      - 默认值：aliyun



## 三、配置示例
```json
{
  "job" : {
    "content" : [ {
      "reader" : {
        "parameter" : {
          "column" : [ {
            "name" : "data",
            "type" : "string"
          } ],
          "sliceRecordCount" : [ "100"]
        },
        "name" : "streamreader"
      },
      "writer" : {
        "name": "odpswriter",
        "parameter": {
          "odpsConfig": {
            "accessId": "${odps.accessId}",
            "accessKey": "${odps.accessKey}",
            "project": "${odps.project}"
          },
          "table": "tableTest",
          "partition": "pt='xx'",
          "writeMode": "append",
          "bufferSize": 64,
          "column": [{
            "name": "col1",
            "type": "string"
          }]
        }
      }
    } ],
    "setting" : {
      "restore" : {
        "isRestore" : false
      },
      "speed" : {
        "channel" : 1
      }
    }
  }
}
```
