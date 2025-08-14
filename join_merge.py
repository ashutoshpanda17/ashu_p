# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

df_emp = spark.read.csv('/Volumes/workspace/default/test_practice/emp.csv',header=True)
df_dept = spark.read.csv('/Volumes/workspace/default/test_practice/dept.csv',header=True)

# COMMAND ----------

df_emp = df_emp.withColumn('hash_key', md5(col('DEPTNO')))
df_emp.display()

# COMMAND ----------

df_dept = df_dept.withColumn('hash_key', md5(col('DEPTNO')))
df_dept.display()

# COMMAND ----------

df_emp.join(df_dept, on='hash_key',how='inner').display()

# COMMAND ----------

df_emp.withColumn('partition_num', spark_partition_id()).display()

# COMMAND ----------

df_emp.repartition(15).withColumn('partition_num', spark_partition_id()).display()

# COMMAND ----------

df_emp.repartition(8,'JOB').display()

# COMMAND ----------

df_emp.repartitionByRange(8,'JOB').display()

# COMMAND ----------

repartition(8) --- #reduce the workload
repartitionByRange(8,'col1') --- #reduce the shuffling and sort the data

# COMMAND ----------

df_pivot = df_emp.groupBy("DEPTNO").pivot('JOB').agg(mean('SAL'))
df_pivot.display()

# COMMAND ----------

expr_unpivot = "stack(5, 'ANALYST', ANALYST, 'CLERK', CLERK, 'MANAGER', MANAGER, 'PRESIDENT' , PRESIDENT,'SALESMAN', SALESMAN) as (JOB,SAL)"
df_unpivot = df_pivot.select('DEPTNO', expr(expr_unpivot))
df_unpivot.display()

# COMMAND ----------

df_date=spark.createDataFrame([(1,'01-11-2022'),(2,'01-03-2020'),(3,'01-08-2019')],['id','date'])

# COMMAND ----------

df_date.display()

# COMMAND ----------

help(date_format)

# COMMAND ----------

df_date = df_date.withColumn('date', to_date('date','dd-MM-yyyy')).withColumn('new_format', date_format('date','MMM-yyyy'))
df_date.display()

# COMMAND ----------

df_date.withColumn('date_diff', date_diff(current_date(),col('date'))).display()

# COMMAND ----------

df_date.withColumn('date_diff', months_between(current_date(),col('date'))).display()

# COMMAND ----------

df_date.withColumn('date_add', date_add(col('date'),2)).display()

# COMMAND ----------

df_date.withColumn('month_add', add_months(col('date'),-2)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## call a notebook

# COMMAND ----------

# MAGIC %run ./Pyspark_transform

# COMMAND ----------

df_date.display()

# COMMAND ----------

from datetime import datetime
def date_format_mod(x):
    return datetime.strftime(x,'%b-%Y')

udf_date = udf(date_format_mod,StringType())
df_date.withColumn('new_format',udf_date(col('date'))).display()

# COMMAND ----------


