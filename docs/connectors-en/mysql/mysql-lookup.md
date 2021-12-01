# Mysql Lookup

## 1. Introduction
Mysql Lookup table，full and asynchronous approaches are supported.<br />
Full cache: Load all dimensional table data into memory, and it is recommended that the amount of data is not used.<br />
Asynchronous caching: Query data using asynchronous means, and cache the queried data to memory using lru, which is recommended for large amounts of data.

## 2. Support Version
mysql5.x


## 3. Plugin Name
| SQL | mysql-x |
| --- | --- |

## 4. Parameter
- **connector**
  - definition：mysql-x
  - necessary：true
  - data type：String
  - default：null
  <br />

- **url**
  - definition：jdbc:mysql://localhost:3306/test
  - necessary：true
  - data type：String
  - default：null
  <br />

- **table-name**
  - definition：the name of table
  - necessary：true
  - data type：String
  - default：null：
  <br />

- **username**
  - definition：username
  - necessary：true
  - data type：String
  - default：null
  <br />

- **password**
  - definition：password
  - necessary：true
  - data type：String
  - default：null
  <br />

- **lookup.cache-type**
  - definition：lookup table cache type(NONE、LRU、ALL)
  - necessary：false
  - data type：string
  - default：LRU
  <br />

- **lookup.cache-period**
  - definition：the time of interval to load data when use ALL lookup table, Unit: ms
  - necessary：false
  - data type：string
  - default：3600000
  <br />

- **lookup.cache.max-rows**
  - definition：the size of data that lru lookup table cache
  - necessary：false
  - data type：string
  - default：10000
  <br />

- **lookup.cache.ttl**
  - definition：the expire time of data that lru lookup table cache
  - necessary：false
  - data type：string
  - default：60000
  <br />

- **lookup.fetch-size**
  - definition：the size of data that ALL lookup table load in every batch
  - necessary：false
  - data type：string
  - default：1000
  <br />

- **lookup.parallelism**
  - definition：parallelism of lookup table
  - necessary：false
  - data type：string
  - default：null
  <br />

## 5. Data type
| Support | BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、FLOAT、DOUBLE、DECIMAL、STRING、VARCHAR、CHAR、TIMESTAMP、DATE、BINARY |
| --- | --- |
| unSupport | ARRAY、MAP、STRUCT、UNION |


## 6. Profile Demo
see`flinkx-examples`directory.
