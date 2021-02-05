# Oracle LogMiner Reader

<!-- TOC -->

- [Oracle LogMiner Reader](#oracle-logminer-reader)
    - [一、插件名称](#一插件名称)
    - [二、支持的数据源版本](#二支持的数据源版本)
    - [三、数据库配置](#三数据库配置)
    - [四、基本原理](#四基本原理)
    - [五、参数说明](#五参数说明)
    - [六、配置示例](#六配置示例)

<!-- /TOC -->

<br/>

## 一、插件名称
名称：**oraclelogminerreader**

<br/>

## 二、支持的数据源版本
**支持Oracle 10，Oracle 11以及Oracle12单机版，不支持RAC模式，暂不支持Oracle18、Oracle19**

<br/>

## 三、数据库配置
[Oracle配置LogMiner](../other/LogMiner配置.md)

<br/>

## 四、基本原理
[FlinkX Oracle LogMiner实时采集基本原理](../other/LogMiner原理.md)

<br/>

## 五、参数说明

- **jdbcUrl**
   - 描述：Oracle数据库的JDBC URL链接
   - 必选：是
   - 字段类型：String
   - 默认值：无

<br/>

- **username**
   - 描述： 用户名
   - 必选：是
   - 字段类型：String
   - 默认值：无

<br/>

- **password**
   - 描述： 密码
   - 必选：是
   - 字段类型：String
   - 默认值：无

<br/>

- **table**
   - 描述： 需要监听的表，格式为：schema.table，多个以,分割，schema不能配置为\*，但table可以配置\*监听指定库下所有的表，如：schema1.table1,schema1.table2,schema2.\*
   - 必选：否，不配置则监听除`SYS`库以外的所有库的所有表变更信息
   - 字段类型：String
   - 默认值：无

<br/>

- **cat**
   - 描述：需要监听的操作数据操作类型，有UPDATE,INSERT,DELETE三种可选，大小写不敏感，多个以,分割
   - 必选：否
   - 字段类型：String
   - 默认值：UPDATE,INSERT,DELETE

<br/>

- **readPosition**
   - 描述：Oracle实时采集的采集起点
   - 可选值：
      - all：    从Oracle数据库中最早的归档日志组开始采集(不建议使用)
      - current：从任务运行时开始采集
      - time：   从指定时间点开始采集
      - scn：    从指定SCN号处开始采集
   - 必选：否
   - 字段类型：String
   - 默认值：current

<br/>

- **startTime**
   - 描述： 指定采集起点的毫秒级时间戳
   - 必选：当`readPosition`为`time`时，该参数必填
   - 字段类型：Long(毫秒级时间戳)
   - 默认值：无

<br/>

- **startSCN**
   - 描述： 指定采集起点的SCN号
   - 必选：当`readPosition`为`scn`时，该参数必填
   - 字段类型：String
   - 默认值：无

<br/>

- **fetchSize**
   - 描述： 批量从v$logmnr_contents视图中拉取的数据条数，对于大数据量的数据变更，调大该值可一定程度上增加任务的读取速度
   - 必选：否
   - 字段类型：Integer
   - 默认值：1000

<br/>

- **queryTimeout**
   - 描述： LogMiner执行查询SQL的超时参数，单位秒
   - 必选：否
   - 字段类型：Long
   - 默认值：300

<br/>

- **supportAutoAddLog**
   - 描述：启动LogMiner是否自动添加日志组
   - 必选：否
   - 字段类型：Boolean
   - 默认值：false

<br/>

- **pavingData**
   - 描述：是否将解析出的json数据拍平
   - 必选：否
   - 字段类型：String
   - 默认值：false（一般配置成true比较好）
   - 示例：假设解析的表为CDC,数据库schema为TUDOU，对CDC中的NAME字段做update操作，NAME原来的值为a，更新后为b，则pavingData为true时数据格式为：
   
      ```json
      {
         "scn": 1807399,
         "type": "UPDATE",
         "schema": "TUDOU",
         "table": "CDC",
         "ts": 6760525407742726144,
         "opTime": "2021-01-28 11:52:02.0",
         "after_NAME": "b",
         "after_ID": "1",
         "after_USER_ID": "1",
         "before_ID": "1",
         "before_USER_ID": "1",
         "before_NAME": "a"
      }
      ```
   - pavingData为false时：
      ```json
      {
      "message": {
         "scn": 1807399,
         "type": "UPDATE",
         "schema": "TUDOU",
         "table": "CDC",
         "ts": 6760525407742726144,
         "opTime": "2021-01-28 11:52:02.0",
         "before": {
            "ID": "1",
            "USER_ID": "1",
            "NAME": "a"
         },
         "after": {
            "NAME": "b",
            "ID": "1",
            "USER_ID": "1"
         }
      }
      }
      ```
      其中：
      
         1、scn：Oracle数据库变更记录对应的scn号
         2、type：变更类型，INSERT，UPDATE、DELETE
         3、opTime：Oracle数据库中数据的变更时间
         4、ts：自增ID，不重复，可用于排序，解码后为FlinkX的事件时间，解码规则如下：

      ```java
      long id = Long.parseLong("6760525407742726144");
      long res = id >> 22;
      DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      System.out.println(sdf.format(res));		//2021-01-28 19:54:21
      ```

<br/>

## 六、配置示例
```json
{
  "job": {
    "content": [
      {
        "reader": {
          "parameter": {
            "jdbcUrl": "jdbc:oracle:thin:@127.0.0.1:1521:xe",
            "username": "kminer",
            "password": "kminerpass",
            "table": "SCHEMA1.*",
            "cat": "UPDATE,INSERT,DELETE",
            "startSCN": "482165",
            "readPosition": "current",
            "startTime": 1576540477000,
            "pavingData": true,
            "queryTimeout": 300
          },
          "name": "oraclelogminerreader"
        },
        "writer": {
          "parameter": {
            "print": true
          },
          "name": "streamwriter"
        }
      }
    ],
    "setting": {
      "restore": {
        "isRestore" : true,
        "isStream" : true
      },
      "speed": {
        "channel": 1
      }
    }
  }
}
```


