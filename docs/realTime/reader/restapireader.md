# Restapi Reader
<!-- TOC -->

- [Restapi Reader](#restapi-reader)
  - [一、插件名称](#一插件名称)
  - [二、参数说明](#二参数说明)
  - [三、配置示例](#三配置示例)

<!-- /TOC -->

## 一、插件名称
名称：restapireader


## 二、参数说明

- url
  - 描述：http请求地址
  - 必选：是
  - 字段类型：string

<br/>

- requestMode
  - 描述：http请求方式
  - 必选：是
  - 字段类型：string
  - 可选值：post get

<br/>

- header
  - 描述: 请求的header
  - 注意: 当请求方式为post时，Content-Type需为application/json，目前只支持post请求的json格式，不支持表单提交
  - 必选: 否，如果requestMode配置未post，header会自动添加 'application/json' head头
  - 字段类型：数组
```json
 "header": [
              {
                "name": "token",
                "value": " ${uuid}"
              }
            ]
```

- 参数解析
  - name 请求的key 必选
  - value key的值 必选

<br/>

- body
  - 描述：对应post请求的body参数
  - 注意：参数支持动态参数替换，内置变量以及动态变量的加减(只支持动态变量的一次加减运算)，
    - 内置变量
      - ${currentTime}当前时间，获取当前时间，格式为yyyy-MM-dd HH:mm:ss类型
      - ${intervalTime}间隔时间，代表参数 intervalTime 的值
      - ${uuid} 随机字符串 32位的随机字符串
    - param/body/response变量
      - ${param.key} 对应get请求param参数里key对应的值
      - ${body.key}对应post请求的body参数里key对应的值
      - ${response.key} 对应返回值里的key对应的值
  - 必选：否
  - 字段类型：数组
```json
"body": [
           {
                "name": "stime",
                "value": "${currentTime}",
                "nextValue": "${body.stime}+${intervalTime}",
                "format": "yyyy-mm-dd hh:mm:ss"
              },
              {
                "name": "etime",
                "value": "${body.stime}+${intervalTime}",
                "format": "yyyy-mm-dd hh:mm:ss"
              }
            ]
```

- 参数解析
  - name 请求的key 必选
  - value key的值 必选
  - nextValue 除第一次请求之外，key对应的值 非必选
  - format 格式化模板 非必选，如果要求请求格式是日期格式，必须填写

<br/>

- param
  - 描述：对应get请求参数
  - 注意：参数支持动态参数替换，内置变量以及动态变量的加减(只支持动态变量的一次加减运算)
    - 内置变量
      - ${currentTime}当前时间，获取当前时间，格式为yyyy-MM-dd HH:mm:ss类型
      - ${intervalTime}间隔时间，代表参数 intervalTime 的值
      - ${uuid} 随机字符串 32位的随机字符串
    - param/body/response变量
      - ${param.key} 对应get请求param参数里key对应的值
      - ${body.key}对应post请求的body参数里key对应的值
      - ${response.key} 对应返回值里的key对应的值
  - 必选：否
  - 字段类型：数组
```json
"param": [
              {
                "name": "stime",
                "value": "${currentTime}",
                "nextValue": "${body.stime}+${intervalTime}",
                "format": "yyyy-mm-dd hh:mm:ss"
              },
              {
                "name": "etime",
                "value": "${body.stime}+${intervalTime}",
                "format": "yyyy-mm-dd hh:mm:ss"
              }
            ]
```

- 参数解析
  - name 请求的key 必选
  - value key的值 必选
  - nextValue 除第一次请求之外，key对应的值 非必选
  - format 格式化模板 非必选，如果要求请求格式是日期格式，必须填写

<br/>

- decode
  - 描述 解码器 返回数据是作为json格式还是text格式处理
  - 必选：否
  - 字段类型：string
```json
"deocode":"json"
```

- 默认值：text
- 可选值：text json
  - text 不做任何处理，返回值直接丢出去
  - json 可以进行定制化输出，指定输出的key，则对返回值解析，获取对应的key以及值 组装新的json数据丢出去

<br/>

- fields
  - 描述：在decode为json时，可以对返回值指定key输出
  - 注意：decode为text模式时，不支持此参数， key以 . 作为层级，多个key用逗号隔开
  - 必选： 否
  - 字段类型：string
  - 示例

fields值为
```
"fields": "msg.key1,msg.key2.key3",
```
返回值为：
```json
{
  "msg": {
    "key1": "value1",
    "key2": {
      "key3": "value2",
      "key4": "value3"
    },
    "key5": 2
  }
}
```
根据fields解析后的值为：
```json
{
  "msg": {
    "key1": "value1",
    "key2": {
      "key3": "value2"
    }
  }
}
```
<br/>

- strategy
  - 描述 定义的key的实际值与value指定值相等时进行对应的逻辑处理
  - 必选 否
  - 字段类型：数组
  - 描述：针对返回类型为json的数据，用户会指定key以及对应的value和处理方式。如果返回数据的对应的key的值正好和用户配置的value相等，则执行对应逻辑。同时用户指定的key可以来自返回值也可以来自param参数值

```json
  "strategy": [
    {
      "key": "${param.pageNumber}",
      "value": "${response.totalPageNum}",
      "handle": "stop"
    }]

```

- 参数解析
  - key 选择对应参数的key,支持的格式为
    - 变量
      - ${param.key}
      - ${body.key}
      - ${response.key}
    - 内置变量
      - ${currentTime}当前时间，获取当前时间，格式为yyyy-MM-dd HH:mm:ss类型
      - ${intervalTime}间隔时间，代表参数 intervalTime 的值
      - ${uuid} 随机字符串 32位的随机字符串
  - value 匹配的值，支持的格式为
    - 常量
    - 变量：
      - ${param.key}
      - ${body.key}
      - ${response.key}
    - 内置变量
      - ${currentTime}当前时间，获取当前时间，格式为yyyy-MM-dd HH:mm:ss类型
      - ${intervalTime}间隔时间，代表参数 intervalTime 的值
      - ${uuid} 随机字符串 32位的随机字符串
  - handle 对应处理逻辑
    - stop 停止任务
    - retry 重试，如果重试三次都失败 任务结束

<br/>

- intervalTime
  - 描述： 每次请求间隔时间，单位毫秒
  - 必选：是
  - 字段类型：long



## 三、配置示例
```json
{
  "job": {
    "content": [
      {
        "reader": {
          "parameter": {
            "url": "http://wwww.a.com",
            "requestMode": "post",
            "decode": "json",
            "intervalTime": 3000,
            "fields": "msg.key1,msg.key2.key3",
            "header": [
              {
                "name": "token",
                "value": "0aeb8fd6-02f9-4c84-836a-301ede439976"
              },
              {
                "name": "Content-Type",
                "value": "application/json"
              }
            ],
            "body": [
              {
                "name": "stime",
                "value": "${currentTime}",
                "nextValue": "${body.stime}+${intervalTime}",
                "format": "yyyy-mm-dd hh:mm:ss"
              },
              {
                "name": "etime",
                "value": "${body.stime}+${intervalTime}",
                "format": "yyyy-mm-dd hh:mm:ss"
              }
            ],
            "strategy": [
              {
                "key": "${response.status}",
                "value": "3000",
                "handle": "stop"
              },
              {
                "key": "${response.currentPage}",
                "value": "${response.totalPage}",
                "handle": "stop"
              }
            ]
          },
          "name": "restapireader"
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
        "isRestore": true,
        "isStream": true
      },
      "speed": {
        "channel": 1
      }
    }
  }
}
```


