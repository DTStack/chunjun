# MongoDB Lookup
## 1. Introduce
MongoDB Lookup

## 2. Version Support
MongoDB 3.4 and above

## 3. Connector Name
| SQL | mongodb-x |
| --- | --- |



## 4. Parameter description

- **connector**
    - Description：mongodb-x
    - Required：required
    - Default：(none)



- **url**
    - Description：URL of MongoDB connection，search [MongoDB Documents](https://docs.mongodb.com/manual/reference/connection-string/) for detail information.
    - Required：optional
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



- **lookup.cache-type**
    - Description：lookup table cache type(NONE、LRU、ALL)
    - Required：optional
    - Default：LRU



- **lookup.cache-period**
    - Description：time of interval ALL lookup table load data, Unit: ms.
    - Required：optional
    - Default：3600000



- **lookup.cache.max-rows**
    - Description：size of data in lru lookup table cache.
    - Required：optional
    - Default：10000



- **lookup.cache.ttl**
    - Description：time of data that lru lookup table cache.
    - Required：optional
    - Default：60000



- **lookup.fetch-size**
    - Description：size of data that ALL lookup table load in every batch.
    - Required：optional
    - Default：1000



- **lookup.parallelism**
    - Description：parallelism of lookup table.
    - Required：optional
    - Default：(none)
    
    
    
## 5. Data Type
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
The details are in chunjun-examples dir.


