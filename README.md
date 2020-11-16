# data-catalog-ddl-generator

This repository is to help engineers to generate table ddl from

1) A specific AWS Glue Database & Table name
2) A specific AWS Glue Database
3) All Tables in AWS Glue Data Catalog

To run this on your local machine, you will need to have to configure aws credential on your local computer.

### Dependency
Please setup [AWS CLI Configuration](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html) on your local machine.

---
### Start
```
python generate_specific_table_ddl.py
```

### Information Input
```
> Enter Database Name temp
> Enter Table Name temp
> Enter Query Output Bucket Name Athena Query Output S3 Bucket
```

### Results
```
Execution ID: bla-bla-bla
QUEUED
SUCCEEDED
Query "SHOW CREATE TABLE temp.temp;" finished.
CREATE EXTERNAL TABLE `temp.temp`(
  `temp0` string COMMENT 'temp0',
  `temp1` bigint COMMENT 'temp1',
  `temp2` string COMMENT 'temp2')
PARTITIONED BY (
  `dt` string COMMENT 'temp partition')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://temp/temp'
TBLPROPERTIES (
  'classification'='parquet',
  'has_encrypted_data'='false',
  'parquet.compress'='SNAPPY')
```
