# Stream Source

## 一、介绍
为了让用户能快速熟悉与使用，ChunJun提供了不需要数据库就能读取数据的Stream reader插件。<br />该插件利用了模拟数据的JMockData框架，能够根据给定的属性生成相应的随机数据，方便用户修改和调试

## 二、支持版本



## 三、插件名称
| Sync | streamsource、streamreader |
| --- | --- |
| SQL | stream-x |


## 四、参数说明
### 1、Sync
- **sliceRecordCount**
  - 描述：每个通道生成的数据条数，不配置此参数或者配置为0，程序会持续生成数据，不会停止
  - 必选：否
  - 参数类型：list
  - 默认值：0
<br />

- **permitsPerSecond**
  - 描述：限制每秒生产的条数，默认不限制
  - 必选：否
  - 参数类型：int
  - 默认值：无
<br />

- **column**
  - 描述：随机Java数据类型的字段信息
  - 格式：一组或多组描述"name"和"type"的json格式
  - 格式说明：
    ```json
    {
      "name": "id",
      "type": "int",
      "value":"7"
    }
    ```
    - "name"属性为用户提供的标识，类似于mysql的列名，必须填写。
    - "tpye" 属性为需要生成的数据类型，可配置以下类型：
      - id：从0开始步长为1的int类型自增ID
      - int，integer
      - byte
      - boolean
      - char，character
      - short
      - long
      - float
      - double
      - date
      - timestamp
      - bigdecimal
      - biginteger
      - int[]
      - byte[]
      - boolean[]
      - char[]，character[]
      - short[]
      - long[]
      - float[]
      - double[]
      - string[]
      - binary
      - string：以上均不匹配时默认为string字符串
  - "value"属性为用户设定的输出值，可以不填。
  - 参数类型：list
  - 默认值：无
<br />

### 2、SQL
- **connector**
  - 描述：stream-x
  - 必选：是
  - 参数类型：String
  - 默认值：无
<br />

- **number-of-rows**
  - 描述：输入条数，默认无限
  - 必选：否
  - 参数类型：String
  - 默认值：无
<br />

- **rows-per-second**
  - 描述：每秒输入条数，默认不限制
  - 必选：否
  - 参数类型：String
  - 默认值：无
<br />

## 五、数据类型
| 支持 | BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、FLOAT、DOUBLE、DECIMAL、STRING、VARCHAR、CHAR、TIMESTAMP、DATE、BINARY |
| --- | --- |
| 暂不支持 | ARRAY、MAP、STRUCT、UNION |


## 六、脚本示例
见项目内`chunjun-examples`文件夹。
