# Http Source

## 一、介绍

http source

## 二、插件名称

| Mode | Name |
| --- | --- |
| SYNC | httpsource, httpreader |
| SQL | http-x |

## 三、参数说明

### 1、Sync

- **protocol**
    - 描述：http请求协议
    - 必选：否
    - 字段类型：String
    - 默认值：https

- **decode**
    - 描述：解码器返回数据，是作为json格式，还是text格式处理；
        - text：不做任何处理，直接返回；
        - json：可以进行定制化输出，指定输出的key，则对返回值解析，获取对应的key以及值 组装新的json数据丢出去；
    - 必选：否
    - 字段类型：String
    - 默认值：text

- **fields**
    - 描述：在decode为json时，可以对返回值指定key输出；当decode为text时，不支持此参数；key以'.'为层级，多个key以','隔开；
    - 必选：否
    - 字段类型：String
    - 默认值：无
    - 示例：
        - fields值为："fields": "msg.key1,msg.key2.key3"
        - 返回值为：{"msg":{"key1":"value1","key2":{"key3":"value2","key4":"value3"},"key5":2}}
        - 根据fields解析之后的值为：{"msg":{"key1":"value1","key2":{"key3":"value2"}}}

- **strategy**
    - 描述：定义的key的实际值与value指定值相等时进行对应的逻辑处理；针对返回类型为json的数据，用户会指定key以及对应的value和处理方式。如果返回数据的对应的key的值正好和用户配置的value相等，则执行对应逻辑。同时用户指定的key可以来自返回值也可以来自param参数值
    - 必选：否
    - 字段类型：数组
    - 默认值：无
    - 示例："strategy":[{"key":"${param.pageNumber}","value":"${response.totalPageNum}","handle":"stop"}]
    - 参数解析
        - key 选择对应参数的key,支持的格式为
            - 变量
                - ${param.key} 对应get请求param参数里key对应的值
                - ${body.key}对应post请求的body参数里key对应的值
                - ${response.key} 对应返回值里的key对应的值
            - 内置变量
                - ${currentTime}当前时间，获取当前时间，格式为yyyy-MM-dd HH:mm:ss类型
                - ${intervalTime}间隔时间，代表参数 intervalTime 的值
                - ${uuid} 随机字符串 32位的随机字符串
        - value 匹配的值，支持的格式为：
            - 常量
            - 变量
                - ${param.key} 对应get请求param参数里key对应的值
                - ${body.key}对应post请求的body参数里key对应的值
                - ${response.key} 对应返回值里的key对应的值
            - 内置变量
                - ${currentTime}当前时间，获取当前时间，格式为yyyy-MM-dd HH:mm:ss类型
                - ${intervalTime}间隔时间，代表参数 intervalTime 的值
                - ${uuid} 随机字符串 32位的随机字符串
        - handle 对应处理逻辑
            - stop：停止任务；
            - retry：重试，如果三次重试都失败，则任务失败；

- **intervalTime**
    - 描述：用户请求间隔时间，单位毫秒
    - 必选：是
    - 字段类型：Long
    - 默认值：无

- **url**
    - 描述：请求url地址
    - 必选：是
    - 字段类型：String
    - 默认值：无

- **method**
    - 描述：请求方式
    - 必选：否
    - 字段类型：String
    - 默认值：post

- **header**
    - 描述：请求头参数
    - 必选：否
    - 字段类型：Map
    - 默认值：无

- **column**
    - 描述：用户请求自定义参数
    - 必选：否
    - 字段类型：List
    - 默认值：无

- **body**
    - 描述：对应post请求的body参数
    - 必选：否
    - 字段类型：数组
    - 注意：参数支持动态参数替换，内置变量以及动态变量的加减(只支持动态变量的一次加减运算)，
        - 内置变量
            - ${currentTime}当前时间，获取当前时间，格式为yyyy-MM-dd HH:mm:ss类型
            - ${intervalTime}间隔时间，代表参数 intervalTime 的值
            - ${uuid} 随机字符串 32位的随机字符串
        - param/body/response变量
            - ${param.key} 对应get请求param参数里key对应的值
            - ${body.key}对应post请求的body参数里key对应的值
            - ${response.key} 对应返回值里的key对应的值
        - 参数解析
            - name：请求的key，必选
            - value：请求key对应的值，必选
            - nextValue：除第一次请求之外，请求Key对应的值，非必选；
            - format：格式化模版，非必选，如果请求体是时间格式，则为必选；
    - 默认值：无

- **param**
    - 描述：对应get请求参数
    - 必选：否
    - 字段类型：数组
    - 注意：参数支持动态参数替换，内置变量以及动态变量的加减（只支持动态变量的一次加减运算）
        - 内置变量
            - ${currentTime}：获取当前时间，格式为yyyy-MM-dd HH:mm:ss类型；
            - ${intervalTime}：间隔时间，代表参数 intervalTime 的值；
            - ${uuid} 随机字符串 32位的随机字符串
        - param/body/response变量
            - ${param.key} 对应get请求param参数里key对应的值
            - ${body.key}对应post请求的body参数里key对应的值
            - ${response.key} 对应返回值里的key对应的值
        - 参数解析
            - name：请求的key，必选
            - value：请求key对应的值，必选
            - nextValue：除第一次请求之外，请求Key对应的值，非必选；
            - format：格式化模版，非必选，如果请求体是时间格式，则为必选；
    - 默认值：无

### 2、SQL

- **url**
    - 描述：请求url地址
    - 必选：是
    - 字段类型：String
    - 默认值：无

- **method**
    - 描述：请求方式
    - 必选：否
    - 字段类型：String
    - 默认值：post

- **header**
    - 描述：请求头参数
    - 必选：否
    - 字段类型：Map
    - 默认值：无

- **column**
    - 描述：用户请求自定义参数
    - 必选：否
    - 字段类型：List
    - 默认值：无

## 四、数据类型

和原生flink数据类型保持一致<br />每种format所支持的数据类型请参考[flink官方文档](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/connectors/formats/)<br />

## 五、脚本示例

见项目内`flinkx-examples`文件夹。

