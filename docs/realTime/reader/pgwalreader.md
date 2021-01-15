# PostgreSQL WAL Reader

<a name="xKxam"></a>
## 一、插件名称
名称：**pgwalreader**<br />

<a name="bNUl5"></a>
## 二、数据源版本
**PostgreSQL数据库版本至少为10.0及以上**<br />

<a name="L08F3"></a>
## 三、使用说明
1、预写日志级别(wal_level)必须为logical<br />2、该插件基于PostgreSQL逻辑复制及逻辑解码功能实现的，因此PostgreSQL账户至少拥有replication权限，若允许创建slot，则至少拥有超级管理员权限<br />3、详细原理请参见[PostgreSQL官方文档](http://postgres.cn/docs/10/index.html)<br />

<a name="0HVLN"></a>
## 四、参数说明<br />

- **jdbcUrl**
  - 描述：PostgreSQL数据库的jdbc连接字符串，参考文档：[PostgreSQL官方文档](https://jdbc.postgresql.org/documentation/head/connect.html)
  - 必选：是
  - 默认值：无



- **username**
  - 描述：数据源的用户名
  - 必选：是
  - 默认值：无



- **password**
  - 描述：数据源指定用户名的密码
  - 必选：是
  - 默认值：无



- **tableList**
  - 描述：需要解析的数据表，格式为schema.table
  - 必选：否
  - 默认值：无



- **cat**
  - 描述：需要解析的数据更新类型，包括insert、update、delete三种
  - 注意：以英文逗号分割的格式填写。
  - 必选：是
  - 默认值：无



- **statusInterval**
  - 描述：复制期间，数据库和使用者定期交换ping消息。如果数据库或客户端在配置的超时时间内未收到ping消息，则复制被视为已停止，并且将引发异常，并且数据库将释放资源。在PostgreSQL中，ping超时由属性wal_sender_timeout配置（默认= 60秒）。可以将pgjdc中的复制流配置为在需要时或按时间间隔发送反馈（ping）。建议比配置的wal_sender_timeout更频繁地向数据库发送反馈（ping）。在生产环境中，我使用等于wal_sender_timeout / 3的值。它避免了网络潜在的问题，并且可以在不因超时而断开连接的情况下传输更改
  - 必选：否
  - 默认值：2000



- **lsn**
  - 描述：要读取PostgreSQL WAL日志序列号的开始位置
  - 必选：否
  - 默认值：0



- **slotName**
  - 描述：复制槽名称，根据该值去寻找或创建复制槽
  - 注意：当allowCreateSlot为false时，该值不能为空
  - 必选：否
  - 默认值：无



- **allowCreateSlot**
  - 描述：是否允许创建复制槽
  - 必选：否
  - 默认值：true



- **temporary**
  - 描述：复制槽是否为临时性的，true：是；false：否
  - 必选：否
  - 默认值：true



- **pavingData**
  - 描述：是否将解析出的json数据拍平
  - 示例：假设解析的表为tb1，schema为dbo，对tb1中的id字段做update操作，id原来的值为1，更新后为2，则pavingData为true时数据格式为：
```json
{
    "type":"update",
    "schema":"dbo",
    "table":"customers",
    "lsn":207967352,
    "ts": 1576487525488,
    "ingestion":1475129582923642,
    "before_id":1,
    "after_id":2
}
```
pavingData为false时：
```json
{
    "type":"update",
    "schema":"dbo",
    "table":"customers",
    "lsn":207967352,
    "ts": 1576487525488,
    "ingestion":1481628798880038,
    "before":{
        "id":1
    },
    "after":{
        "id":2
    }
}
```
其中：ts是数据库中数据的变更时间，ingestion是插件解析这条数据的纳秒时间，lsn是该数据变更的日志序列号

  - 必选：否
  - 默认值：false



<a name="M9wz7"></a>
## 五、配置示例
```json
{
  "job": {
    "content": [{
      "reader" : {
        "parameter" : {
          "username" : "username",
          "password" : "password",
          "url" : "jdbc:postgresql://0.0.0.1:5432/postgres",
          "databaseName" : "postgres",
          "cat" : "update,insert,delete",
          "tableList" : [
            "changepk.test_table"
          ],
          "statusInterval" : 10000,
          "lsn" : 0,
          "slotName" : "",
          "allowCreateSlot" : true,
          "temporary" : true,
          "pavingData" : true
        },
        "name" : "pgwalreader"
      },
      "writer" : {
        "parameter" : {
          "print" : true
        },
        "name" : "streamwriter"
      }
    } ],
    "setting": {
      "speed": {
        "channel": 1,
        "bytes": 0
      },
      "errorLimit": {
        "record": 100
      },
      "restore": {
        "maxRowNumForCheckpoint": 0,
        "isRestore": false,
        "isStream" : true,
        "restoreColumnName": "",
        "restoreColumnIndex": 0
      },
      "log" : {
        "isLogger": false,
        "level" : "debug",
        "path" : "",
        "pattern":""
      }
    }
  }
}
```
<a name="yK0Mt"></a>
## PostgreSQL实时采集原理
PostgreSQL 实时采集是基于 PostgreSQL的逻辑复制以及逻辑解码功能来完成的。逻辑复制同步数据的原理是，在wal日志产生的数据库上，由逻辑解析模块对wal日志进行初步的解析,它的解析结果为ReorderBufferChange（可以简单理解为HeapTupleData），再由pgoutput plugin对中间结果进行过滤和消息化拼接后，然后将其发送到订阅端，订阅端通过逻辑解码功能进行解析。
<a name="xQDDG"></a>
### 版本限制
逻辑复制是pgsql10.0版本之后才支持的，因此此方案只支持10.0之后版本

<a name="nmlSP"></a>
### 主要涉及模块说明
| Logical Decoding | PostgreSQL 的逻辑日志来源于解析物理 WAL 日志。<br />解析 WAL 成为逻辑数据的过程叫 Logical Decoding。 |
| :--- | :--- |
| Replication Slots | 保存逻辑或物理流复制的基础信息。类似 Mysql 的位点信息。<br />一个 逻辑 slot 创建后，它的相关信息可以通过 pg_replication_slots 系统视图获取。<br />如果它在 active 状态，则可以通过系统视图 pg_stat_replication 看到一些 slot 的实时的状态信息。 |
| Output Plugins | PostgreSQL 的逻辑流复制协议开放一组可编程接口，用于自定义输数据到客户端的逻辑数据的格式。<br />这部分实现使用插件的方式被内核集成和使用，称作 Output Plugins。 |
| Exported Snapshots | 当一个逻辑流复制 slot 被创建时，系统会产生一个快照。客户端可以通过它订阅到数据库任意时间点的数据变化。 |



对于修改一条数据之后 ，pgsql订阅端decode解析后的数据格式为
```json
{"id":"schema1.test1",
 "schema":"schema1",
"table":"test1",
 "columnList":[
   {"name":"id","type":"int4","index":0},
   {"name":"name","type":"varchar","index":1}
 ],
 "oldData":["2","23"],
 "newData":["2","name1"],
 "type":"UPDATE",
 "currentLsn":23940928,
 "ts":1596358573614
}
```
主要包含schema table以及类型`INSERT`， `UPDATE`和`DELETE`以及WAL日志id等相关信息<br />
<br />

<a name="p8phn"></a>
### 逻辑复制
逻辑复制使用_发布_和_订阅_模型， 其中一个或多个_订阅者_订阅_发布者_ 节点上的一个或多个_发布_。 订阅者从他们订阅的发布中提取数据,逻辑复制是根据复制标识（通常是主键）复制数据对象及其更改的一种方法，因此在上面订阅端收到消息数据实例中可以发现 具备数据库以及表信息外 还具备修改前数据，修改后数据信息以及执行的type和对应的WAL日志ID

发布可以选择将它们所产生的改变限制在`INSERT`， `UPDATE`和`DELETE`的任意组合上， 类似于触发器被特定事件类型触发。默认情况下，复制所有操作类型。<br />已发布的table必须配置一个“副本标识”以便能够复制 `UPDATE`和`DELETE`操作， 这样可以在订阅者端识别适当的行来更新或删除。默认情况下，这是主键， 如果有的话。另外唯一的索引（有一些额外的要求）也可以被设置为副本标识。 如果表没有任何合适的键，那么它可以设置为复制标识“full”， 这意味着整个行成为键。但是，这是非常低效的， 并且只能在没有其他可能的解决方案时用作后备<br />

<a name="6cbmC"></a>
### 创建发布
为哪些表设置创建一个发布
```sql
CREATE PUBLICATION name
    [ FOR TABLE [ ONLY ] table_name [ * ] [, ...]
      | FOR ALL TABLES ]
    [ WITH ( publication_parameter [= value] [, ... ] ) ]
```


<a name="w5seT"></a>
### WAL日志
WAL 是 Write Ahead Log的缩写,中文称之为预写式日志。WAL log也被简称为xlog，每一次change操作都是先写日志再写数据,保证了事务持久性和数据完整性同时又尽量地避免了频繁IO对性能的影响。WAL的中心概念是**数据文件（存储着表和索引）的修改必须在这些动作被日志记录之后才被写入**<br />WAL日志保存在pg_xlog下，每个xlog文件默认是16MB,为了满足恢复需求，在xlog目录下会产生多个WAL日志，不需要的WAL日志将会被覆盖<br />WAL具备归档功能，通过归档的WAL文件可以恢复数据库到WAL日志覆盖时间内的任意一个时间点的状态并且有了WAL日志之后，逻辑复制就可以在WAL日志生成之后，对其进行一系列操作之后传递给订阅客户端，使得订阅客户端能实时获取到源服务器上的修改数据<br />

<a name="h6CtE"></a>
#### WAL何时被写入
WAL也有个内存缓冲区WAL Buffer，WAL都是先写入缓存中，对于事务操作，缓存的WAL日志是在事务提交的时候写入磁盘的，对于非事务型的由一个异步线程追加进日志文件或者在checkPoint(数据脏页缓存写入磁盘需要先刷新WAL缓存)的时候写入。<br />

<a name="o0UWg"></a>
#### WAL主要配置
```
wal_level 可以选择为minimal, replica, or logical 使用逻辑复制需要设置为logical

fsync boolean类型 表示是否使用fsync()系统调用把WAL文件刷新到物理磁盘，确保数据库在操作系统或硬件奔溃的情况下可恢复到最终状态 默认是on

synchronous_commit boolean类型 声明提交一个事务是否需要等待其把WAL日志写入磁盘后再返回，默认值是’on’

on：默认值，为on且没有开启同步备库的时候,会当wal日志真正刷新到磁盘永久存储后才会返回客户端事务已提交成功,
    当为on且开启了同步备库的时候(设置了synchronous_standby_names),必须要等事务日志刷新到本地磁盘,并且还要等远程备库也提交到磁盘才能返回客户端已经提交.

remote_apply：提交将等待， 直到来自当前同步备用数据库的回复表明它们已收到事务的提交记录并应用它， 以便它对备用数据库上的查询可见。

remote_write：提交将等待，直到来自当前同步的后备服务器的一个回复指示该服务器已经收到了该事务的提交记录并且已经把该记录写出到后备服务器的操作系统。

local：当事务提交时,仅写入本地磁盘即可返回客户端事务提交成功,而不管是否有同步备库。

off：写到缓存中就会向客户端返回提交成功，但也不是一直不刷到磁盘，延迟写入磁盘,延迟的时间为最大3倍的wal_writer_delay参数的(默认200ms)的时间,所有如果即使关闭synchronous_commit,也只会造成最多600ms的事务丢失 可能会造成一些最近已提交的事务丢失，但数据库状态是一致的，就像这些事务已经被干净地中止。但对高并发的小事务系统来说,性能来说提升较大。


wal_sync_method enum类型 用来指定向磁盘强制更新WAL日志数据的方法open_datasync fdatasync fsync_writethrough fsync open_sync



Wal_writer_delay 指定wal writer process 把WAL日志写入磁盘的周期 在每个周期中会先把缓存中的WAL日志刷到磁盘

```


<a name="72pK9"></a>
### 复制槽
每个订阅都将通过一个复制槽接收更改，记录某个订阅者的WAL接收情况。<br />在源数据库写入修改频繁导致WAL日志的写入速度很快，导致大量WAL日志生成，或者订阅者接受日志很慢，在消费远远小于生产的时候,会导致源数据库上的WAL日志还没有传递到备库就被回卷覆盖掉了，如果被覆盖掉的WAL日志文件又没有归档备份，那么订阅者就再也无法消费到此数据。<br />复制槽则保存了此订阅的接收信息，使得未被接收的WAL日日志不会被回收

注意 <br />数据库会记录slot的wal复制位点，并在wal文件夹中保留所有未发送的wal文件，如果客户创建了slot但是后期不再使用就有可能导致数据库的wal日志爆仓，需要及时删除不用的slot<br />
<br />可通过以下SQL获取相关信息
```sql
select * from pg_replication_slots;
```
字段含义
```text
Name            Type        References  Description
slot_name       name        复制槽的唯一的集群范围标识符
plugin          name        正在使用的包含逻辑槽输出插件的共享对象的基本名称，对于物理插槽则为null。
slot_type       text        插槽类型 - 物理或逻辑
datoid          oid         该插槽所关联的数据库的OID，或为空。 只有逻辑插槽才具有关联的数据库。
database        text        该插槽所关联的数据库的名称，或为空。 只有逻辑插槽才具有关联的数据库。
active          boolean     如果此插槽当前正在使用，则为真
active_pid      integer     如果当前正在使用插槽，则使用此插槽的会话的进程ID。 NULL如果不活动。
xmin            xid         此插槽需要数据库保留的最早事务。 VACUUM无法删除任何后来的事务删除的元组。
catalog_xmin    xid         影响该插槽需要数据库保留的系统目录的最早的事务。 VACUUM不能删除任何后来的事务删除的目录元组。
restart_lsn     pg_lsn      最老的WAL的地址（LSN）仍然可能是该插槽的使用者所需要的，因此在检查点期间不会被自动移除
```


<a name="fZCyt"></a>
### 局限性

- 不复制数据库模式和DDL命令。初始模式可以使用`pg_dump --schema-only` 手动复制。后续的模式更改需要手动保持同步。（但是请注意， 两端的架构不需要完全相同。）当实时数据库中的模式定义更改时，逻辑复制是健壮的： 当模式在发布者上发生更改并且复制的数据开始到达订阅者但不符合表模式， 复制将错误，直到模式更新。在很多情况下， 间歇性错误可以通过首先将附加模式更改应用于订阅者来避免。<br />
- 不复制序列数据。序列支持的序列或标识列中的数据当然会作为表的一部分被复制， 但序列本身仍然会显示订阅者的起始值。如果订阅者被用作只读数据库， 那么这通常不成问题。但是，如果打算对订阅者数据库进行某种切换或故障切换， 则需要将序列更新为最新值，方法是从发布者复制当前数据 （可能使用`pg_dump`）或者从表中确定足够高的值。<br />
- 不复制`TRUNCATE`命令。当然，可以通过使用`DELETE` 来解决。为了避免意外的`TRUNCATE`调用，可以撤销表的 `TRUNCATE`权限。<br />
- 不复制大对象 没有什么解决办法，除非在普通表中存储数据。
- 复制只能从基表到基表。也就是说，发布和订阅端的表必须是普通表，而不是视图， 物化视图，分区根表或外部表。对于分区，您可以一对一地复制分区层次结构， 但目前不能复制到不同的分区设置。尝试复制基表以外的表将导致错误



<a name="eBoG2"></a>
### PostgreSQL实时采集配置
<a name="OUZEb"></a>
#### postgresql.conf设置
```
wal_level = logical
```


用于复制链接的角色必须具有`REPLICATION`属性(或者是超级用户) 需要在pg_hba.conf做出如下配置
```
host replication all 10.0.3.0/24 md5
```

<a name="9oFg3"></a>
### 部分核心代码分析


<a name="H1ixL"></a>
#### 执行发布SQL
逻辑复制流是发布/订阅模型，因此生成流之前 先进行发布
```java
public static final String PUBLICATION_NAME = "dtstack_flinkx";
public static final String CREATE_PUBLICATION = "CREATE PUBLICATION %s FOR ALL TABLES;";
public static final String QUERY_PUBLICATION = "SELECT COUNT(1) FROM pg_publication WHERE pubname = '%s';";
   
先执行查找sql 判断是否存在 dtstack_flinkx 的 PUBLICATION
如果不存在 执行创建sql语句
conn.createStatement()
    .execute(String.format(CREATE_PUBLICATION, PUBLICATION_NAME));
```


<a name="1VWgR"></a>
#### 创建一个逻辑复制流
```java
 ChainedLogicalStreamBuilder builder = conn.getReplicationAPI()
                .replicationStream() //定义一个逻辑复制流
                .logical() //级别是logical
                .withSlotName(format.getSlotName())//复制槽名称
                //协议版本。当前仅支持版本1
                .withSlotOption("proto_version", "1")//槽版本号
                //逗号分隔的要订阅的发布名称列表（接收更改）。 单个发布名称被视为标准对象名称，并可根据需要引用
                .withSlotOption("publication_names", PgWalUtil.PUBLICATION_NAME)//关联的发布名称
                .withStatusInterval(format.getStatusInterval(), TimeUnit.MILLISECONDS);
        long lsn = format.getStartLsn();
        if(lsn != 0){
            builder.withStartPosition(LogSequenceNumber.valueOf(lsn));
        }
        stream = builder.start();
```
<a name="mHJLy"></a>
#### 业务处理
逻辑复制流接收到订阅的消息后 进行编码 获取到相应信息处理
```java
  public void run() {
        LOG.info("PgWalListener start running.....");
        try {
            init();
            while (format.isRunning()) {
                //接收到流对象
                ByteBuffer buffer = stream.readPending();
                if (buffer == null) {
                    continue;
                }
                //解码为table对象 具体信息为库 表 字段信息 WAL id等
                //然后就可以对其进行处理了
                Table table = decoder.decode(buffer);
                if(StringUtils.isBlank(table.getId())){
                    continue;
                }
                String type = table.getType().name().toLowerCase();
                if(!cat.contains(type)){
                    continue;
                }
                if(!tableSet.contains(table.getId())){
                    continue;
                }
                LOG.trace("table = {}",gson.toJson(table));
                ...............
            }
        }
  }
```

<br />




