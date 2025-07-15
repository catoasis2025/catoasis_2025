import sys
import json
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, concat_ws, lit, when, expr,max,broadcast,reduce,rank,udf
from pyspark.sql.window import Window
from awsglue.job import Job
import random
import string
import re
from pathlib import Path

from pyspark.sql.functions import udf, lit, col
from pyspark.sql.types import StringType, StructField, StructType, Row
import random
import string
# Get resolved options
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
#output_s3_bucket = args['OUTPUT_S3_BUCKET']
#output_s3_path = f"{output_s3_bucket}"
# Glue and Spark initialization
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Spark datetime config
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


pop_fld_prty = spark.read.csv("s3://fsd-os-dev-master-tables/test_rules/POP_FLD_PRTY_LVL.csv", header=True, inferSchema=True)
rules = spark.read.csv("s3://fsd-os-dev-master-tables/test_rules/rule.csv", header=True, inferSchema=True)
xref_rules = spark.read.csv("s3://fsd-os-dev-master-tables/test_rules/xref_rules.csv", header=True, inferSchema=True)

pop_fld_prty.createOrReplaceTempView("pop_fld_prty")
rules.createOrReplaceTempView("rules")
xref_rules.createOrReplaceTempView("xref_rules")
legal_entity = 'JA'  # we can bring from config
filter_date='2025-04-01'


rules_df =spark.sql(f"""select  c.CTL_FNCL_ORG_CD,
		c.POP_FLD_NM ,
		c.FILE_ID ,
		c.pop_fld_vers ,
		c.sec_by_fac_cd ,
		c.sec_by_mgmt_area ,
		c.eff_dt ,
		c.obs_dt ,
		c.POP_FLD_VAL ,
		c.ASGN_DIST_PCT ,
		c.ASGN_RULE_ID ,
		c.fld_nm, 
		c.fld_mtch_val,
        e.pop_fld_prty,
        e.row_num from (SELECT  d.CTL_FNCL_ORG_CD ,
		d.POP_FLD_NM ,
		d.FILE_ID ,
		d.pop_fld_vers ,
		d.sec_by_fac_cd ,
		d.sec_by_mgmt_area ,
		d.eff_dt ,
		d.obs_dt ,
		d.POP_FLD_VAL ,
		d.ASGN_DIST_PCT ,
		d.ASGN_RULE_ID ,
		x.fld_nm, 
		x.fld_mtch_val 
	FROM (SELECT CTL_FNCL_ORG_CD,
				 POP_FLD_NM,
				 FILE_ID,
				 pop_fld_vers,
				 asgn_rule_id,
				 sec_by_fac_cd,
				 sec_by_mgmt_area,
				 eff_dt,
				 obs_dt,
				 POP_FLD_VAL,
				 ASGN_DIST_PCT
			from rules 
				where to_date(eff_dt,'yyyy-MM-dd') <= to_date('{filter_date}','yyyy-MM-dd') 
				AND to_date(obs_dt,'yyyy-MM-dd') > to_date('{filter_date}','yyyy-MM-dd') and POP_FLD_NM='RBU' ) AS d 
	INNER JOIN 
		(SELECT CTL_FNCL_ORG_CD,
				POP_FLD_NM,
				FILE_ID,
				pop_fld_vers,
				asgn_rule_id,
				sec_by_fac_cd,
				sec_by_mgmt_area,
				eff_dt,
				obs_dt,
				FLD_NM,
				fld_mtch_val 
			from xref_rules
				where to_date(eff_dt,'yyyy-MM-dd') <= to_date('{filter_date}','yyyy-MM-dd') 
				AND to_date(obs_dt,'yyyy-MM-dd') > to_date('{filter_date}','yyyy-MM-dd') and POP_FLD_NM='RBU' ) AS x 
	ON d.CTL_FNCL_ORG_CD = x.CTL_FNCL_ORG_CD 
	and d.pop_fld_vers = x.pop_fld_vers 
	AND d.asgn_rule_id = x.asgn_rule_id 
	AND d.sec_by_fac_cd = x.sec_by_fac_cd 
	and d.FILE_ID = x.FILE_ID
	AND d.sec_by_mgmt_area = x.sec_by_mgmt_area) as c join
    (SELECT  CTL_FNCL_ORG_CD, 
		POP_FLD_NM, 
		FLD_NM, 
		POP_FLD_PRTY, 
		file_id,
		ROW_NUMBER() OVER (PARTITION BY CTL_FNCL_ORG_CD ORDER BY POP_FLD_PRTY ASC) AS row_num 
	FROM(select CTL_FNCL_ORG_CD,
				POP_FLD_NM,
				FLD_NM,
				POP_FLD_PRTY ,
				file_id
			from pop_fld_prty 
			WHERE to_date(eff_dt,'yyyy-MM-dd') <= to_date('{filter_date}','yyyy-MM-dd') 
			AND to_date(obs_dt,'yyyy-MM-dd') > to_date('{filter_date}','yyyy-MM-dd') and POP_FLD_NM='RBU'
          ))e on c.FLD_NM=e.FLD_NM  and c.file_id=e.file_id
          and c.CTL_FNCL_ORG_CD=e.CTL_FNCL_ORG_CD""")
rules_df.write.mode("overwrite").partitionBy("CTL_FNCL_ORG_CD","POP_FLD_NM").parquet(f"s3://fsd-os-dev-master-tables/test_rules/optimized_rules/{filter_date}")
