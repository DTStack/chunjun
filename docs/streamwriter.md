# Stream写入插件（streamwriter）

## 1. 配置样例

```
{
  "job": {
    "content": [
      {
        "reader": {},
        "writer": {
          "parameter": {
            "print":true
          },
          "name": "streamwriter"
        }
      }
    ],
    "setting": {}
  }
}
```

## 2. 参数说明

* **name**
  
  * 描述：插件名，此处填写插件名称，streamwriter，此插件用来单独测试reader插件，对读到的数据不做任务处理；
  
  * 必选：是 
  
  * 默认值：无 

* **print**
  
  * 描述：是否在控制台打印数据
  
  * 必选：否
  
  * 默认值：false

