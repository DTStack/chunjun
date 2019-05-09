# MongoDB写入插件（mongodbwriter）

## 1. 配置样例

```json
{
    "job":{
        "content":[{
            "reader":{},
            "writer":{
                "parameter":{
                    "hostPorts":"localhost:27017",
                    "username": "",
                    "password": "",
                    "database":"test",
                    "collectionName": "test",
                    "writeMode": "insert",
                    "batchSize":1,
                    "column": [
                        {
                            "name":"id",
                            "type":"int",
                            "splitter":","
                        },
                        {
                            "name":"id",
                            "type":"string",
                            "splitter":","
                        }
                    ],
                    "replaceKey":"id"
                },
                "name":"mongodbwriter"
            }
        }]
    }
}
```

## 2. 参数说明

* **name**
  
  * 描述：插件名，此处只能填 mongodbwriter，否则Flinkx将无法正常加载该插件包。
  
  * 必选：是
  
  * 默认值：无

* **hostPorts**
  
  * 描述：MongoDB的地址和端口，格式为 IP1:port，可填写多个地址，以英文逗号分隔。
  
  * 必选：是
  
  * 默认值：无

* **username**
  
  * 描述：数据源的用户名
  
  * 必选：否
  
  * 默认值：无

* **password**
  
  * 描述：数据源指定用户名的密码
  
  * 必选：否
  
  * 默认值：无

* **database**
  
  * 描述：数据库名称
  
  * 必选：是
  
  * 默认值：无

* **collectionName**
  
  * 描述：集合名称
  
  * 必选：是
  
  * 默认值：无

* **column**
  
  * 描述：MongoDB 的文档列名，配置为数组形式表示 MongoDB 的多个列。
    
    - name：Column 的名字。
    - type：Column 的类型。
    - splitter：特殊分隔符，当且仅当要处理的字符串要用分隔符分隔为字符数组 Array 时，才使用这个参数。通过这个参数指定的分隔符，将字符串分隔存储到 MongoDB 的数组中。
  
  * 必选：是
  
  * 默认值：无

* **replaceKey**
  
  * 描述：replaceKey 指定了每行记录的业务主键，用来做覆盖时使用（不支持 replaceKey为多个键，一般是指Monogo中的主键）。
  
  * 必选：否
  
  * 默认值：无

* **writeMode**
  
  * 描述：写入模式，当 batchSize > 1 时不支持 replace 和 update 模式
  
  * 必选：是
  
  * 所有选项：insert/replace/update
  
  * 默认值：insert

* **batchSize**
  
  * 描述：一次性批量提交的记录数大小，该值可以极大减少FlinkX与MongoDB的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成FlinkX运行进程OOM情况。<br />
  
  * 必选：否
  
  * 默认值：1
