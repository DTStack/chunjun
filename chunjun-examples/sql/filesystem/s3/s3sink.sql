CREATE TABLE source (
    name String
) WITH (
      'connector' = 'kafka-x',
      'topic' = 'shifang_s3',
      'properties.bootstrap.servers' = 'chunjun1:9092',
      'scan.startup.mode' = 'earliest-offset',
      'format' = 'json'
      );

CREATE TABLE sink10 (
    name String
) WITH (
      'connector' = 'filesystem-x',
      'path' = 's3a://daishu-csp-test01-1255000139/test/shifang1211/',
      'format' = 'json',
      'sink.rolling-policy.rollover-interval' = '10s',
      'sink.rolling-policy.rollover-interval' = '30min',
      'auto-compaction' = 'true',
      's3.access-key' ='xxxxxxxxxx',
      's3.secret-key' = 'xxxxxxxxxx',
      's3.ssl.enabled' ='false',
      's3.path.style.access' = 'true',
      's3.endpoint'= 'https://xxxxxxx'
      );

insert into sink10 select name from source
