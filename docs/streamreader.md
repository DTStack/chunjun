# Stream读取插件（streamreader）

## 1. 配置样例

```
{
  "job": {
    "content": [
      {
        "reader": {
          "parameter": {
            "column": [
              {
                "type": "int",
                "value":"xxx"
              }
            ],
            "sliceRecordCount":10000
          },
          "name": "streamreader"
        },
        "writer": {}
      }
    ],
    "setting": {}
  }
}
```

## 2. 参数说明

* **name**
  
  * 描述：插件名，此处填写插件名称，streamreader。
  
  * 必选：是 
  
  * 默认值：无 

* **sliceRecordCount**
  
  * 描述：每个通道生成的数据条数，不配置此参数或者配置为0，程序会持续生成数据，不会停止
  
  * 必选：否
  
  * 默认值：0

* **column**
  
  * 描述：需要生成的字段。
  
  * 属性说明:
    
    * type：字段类型，程序根据指定的字段类型生成模拟数据，支持基本数据类型以及基本类型的数组，"int[]"表示生成一个长度随机的整形数组；
    
    * value：常量值，程序使用此字段的值直接返回；
  
  * 必选：是
  
  * 默认值：无
