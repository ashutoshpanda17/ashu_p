# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

spark = SparkSession.builder.appName('Spark_Dataframe').getOrCreate()

# COMMAND ----------

df_emp = spark.createDataFrame(data = [('213','HARIS',29),('214','RASID',39),('215','MIRA',27)], schema= ['EMP_ID','EMP_NAME','EMP_AGE'])
df_emp = df_emp.filter(col('EMP_AGE')>25)
df_emp = df_emp.withColumn('NAME_AGE', concat_ws('_',col('EMP_NAME'),col('EMP_AGE')))

# COMMAND ----------

df_emp.show()

# COMMAND ----------

df_emp.display()

# COMMAND ----------

df_emp.printSchema()

# COMMAND ----------

df_emp.collect()[0]

# COMMAND ----------

df_emp.take(2)

# COMMAND ----------

df_emp.explain()

# COMMAND ----------

df_emp.rdd.map(lambda x : x[2]*2).collect()

# COMMAND ----------

from pyspark.sql.types import StringType,DateType

# COMMAND ----------

schema = StructType([
    StructField('EMP_ID', IntegerType(), False),
    StructField('EMP_NAME', StringType() , False),
    StructField('EMP_AGE', IntegerType() , True)
])

# COMMAND ----------

help(StructField)

# COMMAND ----------

df_emp = spark.createDataFrame(data = [(213,'HARIS',None),(214,'RASID',39),(215,'MIRA',27)], schema = schema)

# COMMAND ----------

df_emp.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Read file in spark

# COMMAND ----------

df_prod = spark.read.format('csv').option('header',True).load('dbfs:/FileStore/tables/candy_production.csv')

# COMMAND ----------

spark.read.option("header", True).schema(
    StructType(
        [
            StructField("observation_date", DateType(), False),
            StructField("IPG3113N", FloatType(), True),
        ]
    )
).csv("dbfs:/FileStore/tables/candy_production.csv").display()

# COMMAND ----------

df_prod.display()

# COMMAND ----------

spark.read.option("header", True).schema(
    StructType(
        [
            StructField("observation_date", DateType(), False),
            StructField("IPG3113N", IntegerType(), True),
            StructField('_corrupt_rec',StringType(),True)
        ]
    )
).option('mode','PERMISSIVE').option('columnNameOfCorruptRecord','_corrupt_rec').csv("dbfs:/FileStore/tables/candy_production.csv").display()

# COMMAND ----------

spark.read.option("header", True).schema(
    StructType(
        [
            StructField("observation_date", DateType(), False),
            StructField("IPG3113N", IntegerType(), True)
            #StructField('_corrupt_rec',StringType(),True)
        ]
    )
).option('mode','DROPMALFORMED').csv("dbfs:/FileStore/tables/candy_production.csv").display()

# COMMAND ----------

spark.read.option("header", True).schema(
    StructType(
        [
            StructField("observation_date", DateType(), False),
            StructField("IPG3113N", IntegerType(), True)
            #StructField('_corrupt_rec',StringType(),True)
        ]
    )
).option('mode','FAILFAST').csv("dbfs:/FileStore/tables/candy_production.csv").display()

# COMMAND ----------

df = spark.read.option("header", True).csv("dbfs:/FileStore/tables/candy_production.csv")
df.display()

# COMMAND ----------

df.select([count(when(col(c).isNull() , c)).alias(c) for c in df.columns]).display()

# COMMAND ----------

df.filter(col('IPG3113N')> 40).display()

# COMMAND ----------

df.filter((df['IPG3113N'] > 40) & (df['observation_date']== '1972-04-01')).display()

# COMMAND ----------

df.rdd.map(lambda x : (x[0],x[1]*2)).collect()

# COMMAND ----------

df.select(to_date(col('observation_date'),'yyyy-MM-dd').alias('observation_date'),"*").display()

# COMMAND ----------

df.select(to_date(col('observation_date'),'yyyy-MM-dd'),col('observation_date').alias('OBSERVTN_DATE'),col('IPG3113N')).display()

# COMMAND ----------

df.withColumn('OBSERVATION_DATE', to_date(col('observation_date'),'yyyy-MM-dd')).withColumn('OBSERVATION_DATE', to_date(col('observation_date'),'yyyy-MM-dd')).display()

# COMMAND ----------

df = df.withColumnRenamed('OBSERVATION_DATE','OBSERVTN_DT')
df.display()

# COMMAND ----------

df.createOrReplaceTempView('df_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df_view where IPG3113N > 80

# COMMAND ----------

df_filter = spark.sql(" select * from df_view where IPG3113N > 80 ")
df_filter.display()

# COMMAND ----------


