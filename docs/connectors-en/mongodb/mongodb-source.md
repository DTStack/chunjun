# MongoDB Source
## 1. Introduce
MongoDb Source


## 2. Version Support
MongoDB 3.4 and above


## 3. Connector Name
| Sync | mongodbsource、mongodbreader |
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
  
  

- **fetchSize**
    - Description：The number of data pieces read each time. Adjust this parameter to optimize the reading rate. Default 0 to let MongoDB Server choose the value itself.
    - Required：optional
    - Type：int
    - Default：0



- **filter**
    - Description：URL of MongoDB connection，search [MongoDB Documents](https://docs.mongodb.com/manual/reference/connection-string/) for detail information.
    - Required：optional
    - Type：String
    - Default：(none)



- **column**
    - Description：columns that should be extract
    - Notes:
        - name：column name 
        - type：column type, It can be different from the field type in the database.
    - Required：required
    - Type：List
    - Default：(none)
#### 4.2 SQL
do not support right now.


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
