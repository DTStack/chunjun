# Http Source

## 一、介绍

支持从http数据源以离线读取接口数据，分为单次拉取数据、多次分页拉取数据

## 二、支持版本

HTTP/1.1 是当前广泛使用的 HTTP 协议版本，它引入了一些改进，包括持久连接、流水线化、范围请求等。
通常，大多数现代的 HTTP 库和服务器都支持 HTTP/1.1。

目前测试的HTTP协议版本也是1.1。


## 三、插件名称

| 支持模式 | 连接器标识  |
|------|--------|
| SQL  | http-x |

## 四、参数说明

### 1、sql模式

- **url**

  - 描述：请求url
  - 必选：是
  - 参数类型：string
  - 默认值：无
    <br />

- **decode**
  - 描述：decode type
  - 注意：对于按照离线模式消费数据的值为：offline-json
  - 必选：是
  - 参数类型：string
  - 默认值：json
    <br />

- **method**
  - 描述：请求方法（post、get等）
  - 必选：是
  - 参数类型：string
  - 默认值：post
    <br />

- **header**

  - 描述：请求的header
  - 必选：否
  - 参数类型：string
  - 默认值：[]
    <br />

- **params**

  - 描述：请求参数
  - 必选：否
  - 参数类型：String
  - 默认值：[]
    <br />

- **returned-data-type**

  - 描述：请求返回数据类型。如：single：单条数据；array：数组数据，会转为多条数据。
  - 必选：是
  - 参数类型：String
  - 默认值：single
    <br />

- **json-path**

  - 描述：按照jsonpath语法，定位和提取json的数据。
    如：样例数据{ "store":{"bicycle":"111"}}，json-path=$.store，则取得是{"bicycle":"111"}
  - 必选：否
  - 参数类型：string
  - 默认值：无
    <br />

- **page-param-name**

  - 描述：分页读取时，分页参数名。例如：pageNum
  - 必选：否
  - 参数类型：String
  - 默认值：无
    <br />

- **startIndex**

  - 描述：分页开始页
  - 
  - 必选：否
  - 参数类型：int
  - 默认值：1
    <br />

- **endIndex**

  - 描述：分页结束页
  - 必选：否
  - 参数类型：int
  - 默认值：1
    <br />


- **step**

  - 描述：请求步长，比如第一次请求为第1页，步长为2，那下次一次请求是第3页
  - 必选：否
  - 参数类型：int
  - 默认值：1
    <br />



## 五、数据类型

| 是否支持 |                             类型名称                              |
| :------: |:-------------------------------------------------------------:|
|  支持  | BOOLEAN、INTEGER、BIGINT、DATE、FLOAT、DOUBLE、CHAR、VARCHAR、DECIMAL |


## 六、脚本示例

见项目内`chunjun-examples`文件夹。
