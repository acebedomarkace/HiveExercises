
-- Create table schema
CREATE TABLE zzs_transaction (msisdn STRING
, brand STRING
, transaction_type STRING
, amount INT
, promo_name STRING
, transaction_date DATE);



-- Create Table From a CSV File in the directory
CREATE TABLE IF NOT EXISTS zzs_transaction (msisdn STRING
, brand STRING
, transaction_type STRING
, amount INT
, promo_name STRING
, transaction_date DATE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/10011881/exer'
TBLPROPERTIES('skip.header.line.count'='1')

-- Create a bucketed table Part  1
-- bucket column: msisdn, num_of_buckets: 4
SET hive.enforce.bucketing=true;
CREATE TABLE zzs_transaction_bucketed1 (msisdn STRING
  , brand STRING
  , amount INT
  , promo_name STRING
  , transaction_date DATE
)
PARTITIONED BY(transaction_type STRING)
CLUSTERED BY (msisdn) INTO 4 BUCKETS;

--populating the bucketed table
SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE zzs_transaction_bucketed1
PARTITION(transaction_type)
SELECT msisdn, brand
  , amount
  , promo_name
  , transaction_date
  , transaction_type
FROM zzs_transaction;

-- sampling data from the bucketed table
SELECT * FROM zzs_transaction_bucketed1
  TABLESAMPLE (BUCKET 1 OUT OF 10 ON msisdn);

-- Create a bucketed table Part  2
-- bucket column: msisdn, num_of_buckets: 4
CREATE TABLE zzs_transaction_bucketed2 (msisdn STRING
  , brand STRING
  , amount INT
  , promo_name STRING
  , transaction_date DATE
)
PARTITIONED BY(transaction_type STRING)
CLUSTERED BY (msisdn) INTO 4 BUCKETS;

--populating the bucketed table
SET hive.enforce.bucketing=true;
INSERT OVERWRITE TABLE zzs_transaction_bucketed2
PARTITION(transaction_type)
  SELECT CONCAT('63',msisdn)
    , brand
    , amount
    , promo_name
    , transaction_date
    , transaction_type
   FROM zzs_transaction;

-- sampling data from the bucketed table
  SELECT * FROM zzs_transaction_bucketed2
    TABLESAMPLE (BUCKET 1 OUT OF 10 ON msisdn);


--Table with only Top up (not bucketed)
--Source: bucketed table 1

--Using CTAS

CREATE TABLE zzs_transaction_topup
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
AS
SELECT msisdn, brand, transaction_type, amount
FROM zzs_transaction_bucketed1
WHERE transaction_type = 'TOPUP'

--Table without Top up (not bucketed)
--Source: bucketed table 2

CREATE TABLE zzs_transaction_no_topup
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
AS
SELECT msisdn, brand, transaction_type, amount
FROM zzs_transaction_bucketed2
WHERE transaction_type != 'TOPUP'


--------------------------------------------------
-- parameters for reducing the number of mappers
-- when populating the bucketed tables
-- https://cwiki.apache.org/confluence/display/Hive/AdminManual+Configuration#AdminManualConfiguration-HiveConfigurationVariables
SET hive.merge.mapfiles = true<default>;
SET hive.merge.mapredfiles = false<default>;
--------------------------------------------------

--------------------------------------------------
--Adding compression; The ff youtube video has
--has been most helpful for this challenge
-- https://www.youtube.com/watch?v=nWHGz1Muxao
--------------------------------------------------

SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;

CREATE TABLE zzs_transaction_topup_comp
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
AS
SELECT msisdn, brand, transaction_type, amount
FROM zzs_transaction_bucketed1
WHERE transaction_type = 'TOPUP'

--------------------------------------------------
-- On the challenge to customize the compressed
-- table file output extension name
--------------------------------------------------
SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;
SET hive.output.file.extension=_transaction.csv.gz;

CREATE TABLE zzs_transaction_topup_comp
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
AS
SELECT msisdn, brand, transaction_type, amount
FROM zzs_transaction_bucketed1
WHERE transaction_type = 'TOPUP'


-----------------------------------------------------
-- Multi-partition, CSV with GZip compression
-----------------------------------------------------
CREATE TABLE zzs_transaction_multipart (msisdn STRING
  , transaction_type STRING
  , transaction_date DATE
  , promo_name STRING
  , amount INT
)
PARTITIONED BY(p_txn_month STRING, p_brand STRING)

SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;
SET hive.output.file.extension=_transaction.csv.gz;
SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE zzs_transaction_multipart
PARTITION(p_txn_month, p_brand)
SELECT msisdn, transaction_type, transaction_date, promo_name, amount,
CASE
WHEN month(transaction_date)=1 then "Jan"
WHEN month(transaction_date)=2 then "Feb"
WHEN month(transaction_date)=3 then "Mar"
WHEN month(transaction_date)=4 then "Apr"
WHEN month(transaction_date)=5 then "May"
WHEN month(transaction_date)=6 then "Jun"
WHEN month(transaction_date)=7 then "Jul"
WHEN month(transaction_date)=8 then "Aug"
WHEN month(transaction_date)=9 then "Sep"
WHEN month(transaction_date)=10 then "Oct"
WHEN month(transaction_date)=11 then "Nov"
WHEN month(transaction_date)=12 then "Dec"
ELSE "none"
END AS p_txn_month
, brand as p_brand
FROM zzs_transaction

--------------------------------------------------
--The following section is my first attempt at
--building the topup no-topup tables
--both were unsatisfactory to my assessment
--------------------------------------------------

--Table with only Top up (not bucketed)
--Source: bucketed table 1

-- First build the table schema
CREATE TABLE zzs_transaction_topup (msisdn STRING
  , brand STRING
  , transaction_type STRING
  , amount INT
)

-- populating the un-bucketed table from a bucketed source
SET hive.exec.compress.output=true;
SET io.seqfile.compression.type=BLOCK;
INSERT OVERWRITE TABLE zzs_transaction_topup
  SELECT msisdn
    , brand
    , transaction_type
    , amount
  FROM zzs_transaction_bucketed1
  WHERE transaction_type = 'TOPUP' ;


  --Table without Top up (not bucketed)
  --Source: bucketed table 2

  -- First build the table schema
  SET hive.exec.compress.output=true;
  CREATE TABLE zzs_transaction_notopup (msisdn STRING
    , brand STRING
    , transaction_type STRING
    , amount INT
  )

  -- populating the un-bucketed table from a bucketed source
  INSERT OVERWRITE TABLE zzs_transaction_notopup
    SELECT msisdn
      , brand
      , transaction_type
      , amount
    FROM zzs_transaction_bucketed2
    WHERE transaction_type != 'TOPUP' ;

-- extra extra, creating swish table for
CREATE EXTERNAL TABLE swish (
  TIMESTAMP timestamp
  , TIME int, OS string, RAT string, TELCO string
  , MCC bigint, MNC bigint, MANUFACTURER string, MODEL string
  , DOWNLOAD_KBPS bigint, UPLOAD_KBPS bigint, SERVER_NAME string
  , SERVER_SPONSOR_NAME string, ISP_NAME string, CLIENT_IP_ADDRESS string
  , CLIENT_CITY string, CLIENT_LATITUDE double, CLIENT_LONGITUDE double
  , PRE_CONNECTION_TYPE bigint, LOCATION_TYPE tinyint
  , LATENCY bigint, POST_CONNECTION_TYPE bigint
  , PLOSS_SENT_A tinyint, PLOSS_RECV_A tinyint, JITTER_A double
  , TEST_ID bigint, DEVICE_ID bigint, GSM_CELL_ID bigint
  , GSM_CELL_ID_CONV string, LTE_BAND_BW string
  , LAC bigint, CGI bigint, GMAPS_FORMATTED_ADDRESS string
  , GMAPS_NAME string, DBM_A double, LEVEL_A double
  , TEST_METHOD_A double, UARFCN_A string, ARFCN_A string
  , EARFCN_A double, SIGNAL_STRING_A string, SIGNAL_CELL_TYPE_A double
  , IMEI_TAC_S double, TEST_CARRIER_A string, TEST_MCC_A double
  , TEST_MNC_A double, SERVER_SELECTION_A string, RSRP_A double
  , RSRQ_A double, RSSNR_A string, CQI_A string
  , CLIENT_IPV6_ADDRESS string, LOCATION_AGE_A bigint
  , OOKLA_DEVICE_NAME_A string, OOKLA_CARRIER_NAME_A string
  , DOWNLOAD_KB_A bigint, UPLOAD_KB_A bigint, REGION string
  , PROVINCE string, MUNICIPALITY string, BALUARTE string
  , CBD string, THOROUGHFARE string, BARANGAY string
  , PROVINCEPSGC bigint, MUNICIPALITYPSGC bigint
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/10011881/speedtest'
TBLPROPERTIES('skip.header.line.count'='1')
