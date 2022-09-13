CREATE TABLE source
(
    i1 INT,
    i2 INT
) WITH (
      'connector' = 's3-x',
      'assessKey' = '',
      'secretKey' = '',
      'bucket' = '',
      'objects' = '[""]',
      'fieldDelimiter' = '|',
      'isFirstLineHeader' = 'false',
      'region' = ''
      );

CREATE TABLE sink
(
    i1 INT,
    i2 INT
) WITH (
      'connector' = 'stream-x',
      'print' = 'true'
      );

INSERT INTO sink
SELECT *
FROM source;
