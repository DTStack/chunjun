# Http sink

## 一、介绍
Http sink

## 二、插件名称

| Mode | Name |
| --- | --- |
| SYNC | httpsink, httpwriter |
| SQL | http-x |



## 三、参数说明

### 1、Sync
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


## 四、脚本示例
见项目内`chunjun-examples`文件夹。
