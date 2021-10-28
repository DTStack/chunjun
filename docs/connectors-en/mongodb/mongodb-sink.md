# MongoDB Sink
## 1. Introduce
MongoDB Sink

## 2. Version Support
MongoDB 3.4 and above


## 3. Connector Name
| Sync | mongodbsink, mongodbwriter |
| --- | --- |
| SQL | mongodb-x |



## 4. Parameter description
#### 4.1 Sync

- **url**
    - Description：URL of MongoDB connection，search [MongoDB Documents](https://docs.mongodb.com/manual/reference/connection-string/) for detail information.
    - Required：optional
    - Type：String
    - Default：(none)



- **hostPorts**
    - Description：host and port of database, formatted like IP1:port. if using Multiple addresses, separated it by comma.
    - Required：optional
    - Type：String
    - Default：(none)



- **username**
    - Description：user of database
    - Required：optional
    - Type：String
    - Default：(none)



- **password**
    - Description：password of database 
    - Required：optional
    - Type：String
    - Default：(none)



- **database**
    - Description：name of database
    - Required：required
    - Type：String
    - Default：(none)



- **collectionName**
    - Description：collection name of database
    - Required：required
    - Type：String
    - Default：(none)



- **replaceKey**
    - Description：replaceKey specifies the primary key of each row of records, it's useful in replace and update mode.
    - Required：optional
    - Type：String
    - Default：(none)



- **writeMode**
    - Description：write data mode, do not support replace and update mode when batchSize > 1.
    - Required：optional
    - Option：insert/replace/update
    - Type：String
    - Default：insert



- **batchSize**
    - Description：the size of rows in every single batch, it will decrease network communication with MongoDB, but a large number will cause system out of memory.
    - Required：optional
    - Type：int
    - Default：1
    
    
    
- **flushIntervalMills**
    - Description：time interval between each batch, Unit: ms.
    - Required：optional
    - Type：int
    - Default：10000
    
    
    
### 2、SQL
SQL only support INSERT mode. in the future, we will support upsert mode if you configure the primary key.

- **url**
    - Description：URL of MongoDB connection，search [MongoDB Documents](https://docs.mongodb.com/manual/reference/connection-string/) for detail information.
    - Required：optional
    - Type：String
    - Default：(none)
    
    
    
- **database**
    - Description：name of database
    - Required：required
    - Type：String
    - Default：(none)
    
    
    
- **collection**
    - Description：collection name
    - Required：required
    - Default：(none)
    
    
    
- **username**
    - Description：user of database
    - Required：optional
    - Type：String
    - Default：(none)



- **password**
    - Description：password of database 
    - Required：optional
    - Type：String
    - Default：(none)



- **sink.parallelism**
    - Description：parallelism of sink
    - Required：optional
    - Default：(none)
    
    
    
- **sink.buffer-flush.max-rows**
    - Description：the size of rows in every single batch
    - Required：optional
    - Default：(none)
    
    
    
- **sink.buffer-flush.interval**
    - Description：time interval between each batch, Unit: ms.
    - Required：optional
    - Default：(none)
    
    
    
## 5. Type
| support | int |
| --- | --- |
|  | long |
|  | double |
|  | decimal |
|  | objectId |
|  | string |
|  | bindata |
|  | date |
|  | timestamp |
|  | bool |
| no support | array |


## 6. Example
The details are in flinkx-examples dir.

