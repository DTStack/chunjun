# S3 Writer



## 一、插件名称

名称：**s3writer**



## 二、数据源版本

amazon s3 **所有版本**



## 三、参数说明

-  **accessKey**
    - 描述：aws 用户凭证：aws_access_key_id
    - 必选：是
    - 默认值：无



-  **secretKey**
    - 描述：aws 用户凭证：aws_secret_access_key
    - 必选：是
    - 默认值：无



-  **endpoint**
    - 描述：若需指定endpoint，则可通过该参数制定，详情可参见官方文档
      [https://docs.aws.amazon.com/zh_cn/general/latest/gr/rande.html](https://docs.aws.amazon.com/zh_cn/general/latest/gr/rande.html)
    - 必选：否
    - 默认值：根据 region 自动选择 endpoint



-  **region**
    - 描述：储存桶的区域
    - 必选：否
    - 默认值：`cn-north-1`



-  **bucket**
    - 描述：存储桶名称
    - 必选：是
    - 默认值：无



-  **object**
    - 描述：需要写入的对象,只支持一个对象
    - 必选：是
    - 默认值：无
    - 格式：
        - "abc.xml"
        - "xxx/abd"



-  **fieldDelimiter**
    - 描述：写入的字段分隔符,只支持单字符或转义字符
    - 必选：是
    - 默认值：`,`



-  **encoding**
    - 描述：写入文件的编码配置
    - 必选：否
    - 默认值：`UTF-8`



-  **isFirstLineHeader**
    - 描述：是否增加首行为标题行，如果是，将设置第一行为标题
    - 必选：否
    - 默认值：false




-  **column**
    - 描述：需要读取的字段
    - 格式：指定具体信息：
    ```json
    "column": [{
        "name": "col1",
        "type": "datetime"
    }]
    ```

- 属性说明:
    - name：字段名称
    - type：字段类型，s3写入的为文本文件，本质上都是要转化成字符串类型，这里可以指定要转换的字段转换之前的类型



- 必选：是
- 默认值：无
- **writeMode**
    - **描述：写入模式，只支持覆写**
    - **必选：否**
    - **默认值：overwrite**

**chunjun1.12 目前只支持写入string类型,只支持单并行度写入**

## 五、使用示例



```json
{
    "job": {
        "content": [
            {
                "reader": {
                    "parameter": {
                        "column": [
                            {
                                "name": "col1",
                                "type": "string"
                            },
                            {
                                "name": "col2",
                                "type": "string"
                            },
                            {
                                "name": "col3",
                                "type": "int"
                            },
                            {
                                "name": "col4",
                                "type": "int"
                            }
                        ],
                        "sliceRecordCount": [
                            "100"
                        ]
                    },
                    "name": "streamreader"
                },
                "writer": {
                    "parameter": {
                        "accessKey": "",
                        "secretKey": "",
                        "endpoint": "http://127.0.0.1:9090",
                        "region": "",
                        "bucket": "",
                        "object": "aaa.xml",
                        "column": [
                            {
                                "name": "col1",
                                "type": "string"
                            },
                            {
                                "name": "col2",
                                "type": "string"
                            },
                            {
                                "name": "col3",
                                "type": "int"
                            },
                            {
                                "name": "col4",
                                "type": "int"
                            }
                        ],
                        "fieldDelimiter": ",",
                        "encoding": "utf-8",
                        "isFirstLineHeader": true
                    },
                    "name": "s3writer"
                }
            }
        ],
        "setting": {
            "restore": {
                "maxRowNumForCheckpoint": 0,
                "isRestore": false,
                "restoreColumnName": "",
                "restoreColumnIndex": 0
            },
            "errorLimit": {
                "record": 100
            },
            "speed": {
                "bytes": 0,
                "channel": 1
            }
        }
    }
}
```

