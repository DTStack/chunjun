### 如何添加一个E2E测试

#### 1.在`com.dtstack.chunjun.connector.test.standalone` package下创建一个类
```java
import lombok.extern.slf4j.Slf4j;// 必须继承ChunjunFlinkStandaloneTestEnvironment类
@Slf4j
public class PostgreSyncE2eITCase extends ChunjunFlinkStandaloneTestEnvironment {
    
    protected static final String POSTGRE_HOST = "chunjun-e2e-postgre";

    private static final URL POSTGRE_INIT_SQL_URL =
            PostgreSyncE2eITCase.class.getClassLoader().getResource("docker/postgre/init.sql");

    public PostgreContainer postgre;

    // 初始化
    @Override
    public void before() throws Exception {
        super.before();
        log.info("Starting containers...");
        postgre = new PostgreContainer();
        postgre.withNetwork(NETWORK);
        postgre.withNetworkAliases(POSTGRE_HOST);
        postgre.withLogConsumer(new Slf4jLogConsumer(log));
        Startables.deepStart(Stream.of(postgre)).join();
        Thread.sleep(5000);
        initPostgre();
        log.info("Containers are started.");
    }

    // 关闭
    @Override
    public void after() {
        if (postgre != null) {
            postgre.stop();
        }
        super.after();
    }

    // 测试的方法    
    @Test
    public void testPostgreToPostgre() throws Exception {
        submitSyncJobOnStandLone(
                ChunjunFlinkStandaloneTestEnvironment.CHUNJUN_HOME
                        + "/chunjun-examples/json/postgresql/postgre_postgre.json");
        JobAccumulatorResult jobAccumulatorResult = waitUntilJobFinished(Duration.ofMinutes(30));

        Assert.assertEquals(jobAccumulatorResult.getNumRead(), 9);
        Assert.assertEquals(jobAccumulatorResult.getNumWrite(), 9);
        JdbcProxy proxy =
                new JdbcProxy(
                        postgre.getJdbcUrl(),
                        postgre.getUsername(),
                        postgre.getPassword(),
                        postgre.getDriverClassName());
        List<String> expectResult =
                Arrays.asList(
                        "101,scooter,Small 2-wheel scooter,3.14",
                        "102,car battery,12V car battery,8.1",
                        "103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8",
                        "104,hammer,12oz carpenter's hammer,0.75",
                        "105,hammer,14oz carpenter's hammer,0.875",
                        "106,hammer,16oz carpenter's hammer,1.0",
                        "107,rocks,box of assorted rocks,5.3",
                        "108,jacket,water resistent black wind breaker,0.1",
                        "109,spare tire,24 inch spare tire,22.2");
        proxy.checkResultWithTimeout(
                expectResult,
                "inventory.products_sink",
                new String[] {"id", "name", "description", "weight"},
                60000L);
    }
}
```

#### 2.在`com.dtstack.chunjun.connector.containers` package下创建对应测试数据库的container
```java
public class PostgreContainer extends JdbcDatabaseContainer {
    private static final URL POSTGRE_DOCKERFILE =
            PostgreContainer.class.getClassLoader().getResource("docker/postgre/Dockerfile");
    
    public PostgreContainer() throws URISyntaxException {
        super(
                new ImageFromDockerfile(POSTGRE_HOST, true)
                        .withDockerfile(Paths.get(POSTGRE_DOCKERFILE.toURI())));
        withExposedPorts(POSTGRESQL_PORT);
        waitingFor(
                new WaitStrategy() {
                    @Override
                    public void waitUntilReady(WaitStrategyTarget waitStrategyTarget) {}

                    @Override
                    public WaitStrategy withStartupTimeout(Duration startupTimeout) {
                        return null;
                    }
                });
    }

    @Override
    public String getDriverClassName() {
        return PG_DRIVER_CLASS;
    }

    @Override
    public String getJdbcUrl() {
        String additionalUrlParams = this.constructUrlParameters("?", "&");
        return "jdbc:postgresql://"
                + this.getContainerIpAddress()
                + ":"
                + this.getMappedPort(POSTGRESQL_PORT)
                + "/"
                + PG_TEST_DATABASE
                + additionalUrlParams;
    }
}
```

#### 3.在`resources/docker`下添加对应的dockerfile和数据预先写入
```dockerfile
FROM debezium/postgres:9.6

LABEL maintainer="chunjun@github.com"

ENV LANG=C.UTF-8
ENV TZ=Asia/Shanghai
ENV POSTGRES_DB=postgre
ENV POSTGRES_USER=postgre
ENV POSTGRES_PASSWORD=postgre
```
#### 4.在`chunjun-examples`模块添加自己测试的json文件
```json
{
    "job": {
      "content": [
        {
          "reader": {
            "name": "postgresqlreader",
            "parameter": {
              "column": [
                {
                  "name": "id",
                  "type": "serial"
                },
                {
                    "name": "name",
                    "type": "varchar"
                },
                {
                    "name": "description",
                    "type": "varchar"
                },
                {
                    "name": "weight",
                    "type": "float"
                }
              ],
              "username": "postgre",
              "password": "postgre",
              "connection": [
                {
                  "jdbcUrl": [
                    "jdbc:postgresql://chunjun-e2e-postgre:5432/postgre?useSSL=false"
                  ],
                  "table": [
                    "inventory.products"
                  ]
                }
              ]
            }
          },
          "writer": {
            "name": "postgresqlwriter",
            "parameter": {
              "username": "postgre",
              "password": "postgre",
              "connection": [
                {
                  "jdbcUrl": "jdbc:postgresql://chunjun-e2e-postgre:5432/postgre?useSSL=false",
                  "table": [
                    "inventory.products_sink"
                  ]
                }
              ],
              "writeMode": "insert",
              "column": [
                {
                  "name": "id",
                  "type": "serial"
                },
                {
                  "name": "name",
                  "type": "varchar"
                },
                {
                  "name": "description",
                  "type": "varchar"
                },
                {
                  "name": "weight",
                  "type": "float"
                }
              ]
            }
          }
        }
      ],
      "setting": {
        "speed": {
          "channel": 1,
          "bytes": 0
        }
      }
    }
  }

```
