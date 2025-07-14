from pyspark.sql.functions import lit, udf
from pyspark.sql.types import StringType, StructType, StructField
import random
import string

def add_allocation_id(spark, alloc_glue_frame,dataframe_lineage, allocation_lvl,rule_type):
    print(allocation_lvl)
    if allocation_lvl == '1':
        print(f"Lineage started for {rule_type}")
        if "allocation_id" in alloc_glue_frame.columns:
            alloc_glue_frame = alloc_glue_frame.drop("allocation_id")

        alloc_glue_frame = add_random_id(spark, alloc_glue_frame, "allocation_id", 10)
        selected_df = alloc_glue_frame.select(
            "transaction_id",
            "allocation_id",
            "rule_id",
            "POP_FLD_VAL",
            "POP_FLD_PRTY",
            "ASGN_DIST_PCT",
            "JE_DR_AMT",
            "SRC_JE_DR_AMT",
            "SRC_AMT",
            "FLD_NM",
            "FILE_ID",
            "FLD_MTCH_VAL"
        ).withColumn("parent_allocation_id", lit(None).cast("string")) \
            .withColumn("allocation_lvl", lit(allocation_lvl).cast("string")) \
            .withColumn("allocation_rule", lit(rule_type).cast("string")) \
            .select("transaction_id", "parent_allocation_id", "allocation_id", "rule_id", "POP_FLD_VAL","POP_FLD_PRTY",
                    "allocation_lvl", "allocation_rule","ASGN_DIST_PCT","JE_DR_AMT","SRC_JE_DR_AMT","SRC_AMT","FLD_NM","FILE_ID","FLD_MTCH_VAL")

    else:
        print(f"Lineage started for {rule_type}")

        alloc_glue_frame = alloc_glue_frame.drop("parent_allocation_id")
        alloc_glue_frame = alloc_glue_frame.withColumnRenamed("allocation_id", "parent_allocation_id")
        alloc_glue_frame = alloc_glue_frame.drop("allocation_id")
        alloc_glue_frame = add_random_id(spark, alloc_glue_frame, "allocation_id",20)
        if "parent_allocation_id" not in alloc_glue_frame.columns:
            alloc_glue_frame = alloc_glue_frame.withColumn("parent_allocation_id", lit(None).cast("string"))
        selected_df = alloc_glue_frame.select(
            "transaction_id",
            "allocation_id",
            "parent_allocation_id",
            "rule_id",
            "POP_FLD_VAL",
            "POP_FLD_PRTY",
            "ASGN_DIST_PCT",
            "JE_DR_AMT",
            "SRC_JE_DR_AMT",
            "SRC_AMT",
            "FLD_NM",
            "FILE_ID",
            "FLD_MTCH_VAL"
        ).withColumn("allocation_lvl", lit(allocation_lvl).cast("string")) \
            .withColumn("allocation_rule", lit(rule_type).cast("string")) \
            .select("transaction_id", "parent_allocation_id", "allocation_id", "rule_id", "POP_FLD_VAL","POP_FLD_PRTY",
                    "allocation_lvl", "allocation_rule","ASGN_DIST_PCT","JE_DR_AMT","SRC_JE_DR_AMT","SRC_AMT","FLD_NM","FILE_ID","FLD_MTCH_VAL")
    print(f"Lineage returned df for {rule_type}")
    return alloc_glue_frame, selected_df


def generate_code(length=20):
    characters = string.ascii_uppercase + string.digits
    return str(''.join(random.choices(characters, k=length)))


def add_random_id(spark, df, column_name, length):
    random_id_udf = udf(generate_code, StringType())
    return df.withColumn(column_name, random_id_udf())

def create_lineage_df(spark):
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("parent_allocation_id", StringType(), True),
        StructField("allocation_id", StringType(), True),
        StructField("rule_id", StringType(), True),
        StructField("POP_FLD_VAL", StringType(), True),
        StructField("POP_FLD_PRTY", StringType(), True),
        StructField("allocation_lvl", StringType(), True),
        StructField("allocation_rule", StringType(), True),
        StructField("ASGN_DIST_PCT", StringType(), True),
        StructField("JE_DR_AMT", StringType(), True),
        StructField("SRC_JE_DR_AMT", StringType(), True),
        StructField("SRC_AMT", StringType(), True),
        StructField("FLD_NM", StringType(), True),
        StructField("FILE_ID", StringType(), True),
        StructField("FLD_MTCH_VAL", StringType(), True)
    ])


    lineage_df = spark.createDataFrame([], schema)
    return lineage_df