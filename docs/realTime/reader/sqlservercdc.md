# SqlServer CDC Reader
<!-- TOC -->

- [SqlServer CDC Reader](#sqlserver-cdc-reader)
  - [一、插件名称](#一插件名称)
  - [二、数据源版本](#二数据源版本)
  - [三、数据源配置](#三数据源配置)
  - [四、基本原理](#四基本原理)
  - [五、参数说明](#五参数说明)
  - [六、配置示例](#六配置示例)

<!-- /TOC -->

## 一、插件名称
名称：**sqlservercdcreader**


## 二、数据源版本
SqlServer 2012及以上

## 三、数据源配置
[SqlServer配置CDC](../other/SqlserverCDC配置.md)

## 四、基本原理
[FlinkX Sqlserver CDC实时采集基本原理](../other/SqlserverCDC原理.md)

## 五、参数说明


- **url**
  - 描述：SqlServer数据库的jdbc连接字符串，参考文档：[SqlServer官方文档](https://docs.microsoft.com/zh-cn/sql/connect/jdbc/overview-of-the-jdbc-driver?view=sql-server-2017)
  - 必选：是
  - 字段类型：string
  - 默认值：无

<br/>

- **username**
  - 描述：数据源的用户名
  - 必选：是
  - 字段类型：string
  - 默认值：无

<br/>

- **password**
  - 描述：数据源指定用户名的密码
  - 必选：是
  - 字段类型：string
  - 默认值：无

<br/>

- **databaseName**
  - 描述：监听的数据库
  - 必选：是
  - 字段类型：string
  - 默认值：无

<br/>

- **tableList**
  - 描述：需要解析的数据表，表必须已启用CDC，格式为schema.table
  - 必选：否
  - 字段类型：list
  - 默认值：无

<br/>

- **cat**
  - 描述：需要解析的数据更新类型，包括insert、update、delete三种
  - 注意：以英文逗号分割的格式填写。
  - 必选：是
  - 字段类型：string
  - 默认值：无

<br/>

- **pollInterval**
  - 描述：监听拉取SqlServer CDC数据库间隔时间
  - 注意：该值越小，采集延迟时间越小，给数据库的访问压力越大
  - 必选：否
  - 字段类型：long
  - 默认值：1000

<br/>

- **lsn**
  - 描述：要读取SqlServer CDC日志序列号的开始位置
  - 必选：否
  - 字段类型：string
  - 默认值：无

<br/>


- **pavingData**
  - 描述：是否将解析出的json数据拍平
  - 示例：假设解析的表为tb1，schema为dbo，对tb1中的id字段做update操作，id原来的值为1，更新后为2，则pavingData为true时数据格式为：
```json
{
    "type":"update",
    "schema":"dbo",
    "table":"tb1",
    "lsn":"00000032:00002038:0005",
    "ts": 6760525407742726144,
    "before_id":1,
    "after_id":2
}
```
pavingData为false时：
```json
{
    "type":"update",
    "schema":"dbo",
    "table":"tb1",
    "lsn":"00000032:00004a38:0007",
    "ts": 6760525407742726144,
    "before":{
        "id":1
    },
    "after":{
        "id":2
    }
}
```
- type：变更类型，INSERT，UPDATE、DELETE
- lsn：Sqlserver数据库变更记录对应的lsn号
- ts：自增ID，不重复，可用于排序，解码后为FlinkX的事件时间，解码规则如下：

```java
long id = Long.parseLong("6760525407742726144");
        long res = id >> 22;
        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(sdf.format(res));		//2021-01-28 19:54:21
```


- 必选：否
- 字段类型：boolean
- 默认值：false




## 六、配置示例
```json
{
  "job" : {
    "content" : [ {
      "reader" : {
        "parameter" : {
          "username" : "uname",
          "password" : "passwd",
          "url": "jdbc:sqlserver://host:1433;database=databaseName",
          "databaseName":"databaseName",
          "tableList": ["dbo.cdc"],
          "lsn": "00000025:00000bc0:0003",
          "cat": "insert,update,delete"
        },

        "name" : "sqlservercdcreader"
      },
      "writer": {
        "name": "streamwriter",
        "parameter": {
          "print": true
        }
      }
    }
    ],
    "setting" : {
      "restore": {
        "isStream": true
      },
      "speed" : {
        "channel" : 1
      }
    }
  }
}
```


