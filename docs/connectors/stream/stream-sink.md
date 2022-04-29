# Stream Sink

## 一、介绍

控制台显示数据，方便调试

## 二、支持版本

## 三、插件名称

| Sync | streamsink、streamwriter |
| --- | --- |
| SQL | stream-x |

## 四、参数说明

### 1、Sync

- **print**
    - 描述：是否打印
    - 必选：否
    - 参数类型：boolean
    - 默认值：是
      <br />

### 2、SQL

- **connector**
    - 描述：stream-x
    - 必选：是
    - 参数类型：String
    - 默认值：无
      <br />

- **print**
    - 描述：是否打印
    - 必选：否
    - 参数类型：boolean
    - 默认值：是
      <br />

## 五、数据类型

| 支持 | BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、FLOAT、DOUBLE、DECIMAL、STRING、VARCHAR、CHAR、TIMESTAMP、DATE、BINARY |
| --- | --- |
| 暂不支持 | ARRAY、MAP、STRUCT、UNION |

## 六、脚本示例

见项目内`flinkx-examples`文件夹。
