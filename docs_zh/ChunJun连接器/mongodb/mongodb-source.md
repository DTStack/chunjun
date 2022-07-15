## 一、介绍
读取MongoDB数据，目前不支持SQL Scan方式读取MongoDB。


## 二、支持版本
MongoDB 3.4及以上


## 三、插件名称
| Sync | mongodbsource、mongodbreader |
| --- | --- |
| SQL | mongodb-x |



## 四、参数说明
#### 1、数据同步

- **url**
    - 描述：MongoDB数据库连接的URL字符串，详细请参考[MongoDB官方文档](https://docs.mongodb.com/manual/reference/connection-string/)
    - 必选：否
    - 字段类型：String
    - 默认值：无



- **hostPorts**
    - 描述：MongoDB的地址和端口，格式为 IP1:port，可填写多个地址，以英文逗号分隔
    - 必选：否
    - 字段类型：String
    - 默认值：无



- **username**
    - 描述：数据源的用户名
    - 必选：否
    - 字段类型：String
    - 默认值：无



- **password**
    - 描述：数据源指定用户名的密码
    - 必选：否
    - 字段类型：String
    - 默认值：无



- **database**
    - 描述：数据库名称
    - 必选：否
    - 字段类型：String
    - 默认值：无



- **collectionName**
    - 描述：集合名称
    - 必选：是
    - 字段类型：String
    - 默认值：无
    -

- **fetchSize**
    - 描述：每次读取的数据条数，通过调整此参数来优化读取速率。默认为0代表MongoDB服务器自动选择合适的批量大小
    - 必选：否
    - 字段类型：int
    - 默认值：0



- **filter**
    - 描述：过滤条件，采用json格式，通过该配置型来限制返回 MongoDB 数据范围，语法请参考[MongoDB查询语法](https://docs.mongodb.com/manual/crud/#read-operations)
    - 必选：否
    - 字段类型：String
    - 默认值：无



- **column**
    - 描述：需要读取的字段。
    - 属性说明:
        - name：字段名称
        - type：字段类型，可以和数据库里的字段类型不一样，程序会做一次类型转换
    - 必选：是
    - 字段类型：List
    - 默认值：无
#### 2、SQL计算
暂不支持
## 五、数据类型
| 是否支持 | 类型名称 |
| --- | --- |
| 支持 | long  double  decimal objectId string bindata date timestamp bool |
| 不支持 | array |

## 六、脚本示例
见项目内`chunjun-examples`文件夹。
