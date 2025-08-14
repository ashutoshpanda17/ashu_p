# Databricks notebook source
scd type 1--only keep current record or latest record
scd type 2- it keeps current record and history record
scd type 3- keeps only current and previous record

# COMMAND ----------

# MAGIC %md
# MAGIC ## scd type 1

# COMMAND ----------



# COMMAND ----------

'''merge into target using source
on source.col1=target.col1
when matched then update set *
when not matched then insert *
'''

# COMMAND ----------

scd type 1

# COMMAND ----------

spark.createDataFrame([(1,'ram',23),(2,'jon',34),(3,'kim',47)],schema=StructType([StructField('id',IntegerType()),StructField('name',StringType()),StructField('age',IntegerType())])).write.mode('overwrite').saveAsTable('workspace.default.info')

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta.tables import DeltaTable

# COMMAND ----------

d_target=DeltaTable.forName(spark,'info')

# COMMAND ----------

d_target.toDF().display()

# COMMAND ----------

df_source=spark.createDataFrame([(1,'ram',23),(2,'jon',25),(3,'kim',26),(5,'rose',23)],schema=StructType([StructField('id',IntegerType()),StructField('name',StringType()),StructField('age',IntegerType())]))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

data = [
    (1, "Amit", "Sales"),
    (2, "Sneha", "Marketing"),
    (3, "Rahul", "Finance"),
    (4, "Priya", "HR")
]

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department",

# COMMAND ----------

df_source.display()

# COMMAND ----------

d_target.alias('target').merge(df_source.alias('source'),'target.id=source.id').whenMatchedUpdate(set={'age':'source.age'}).whenNotMatchedInsert(values={'id':'source.id','name':'source.name'}).execute() 



# COMMAND ----------

d_target.toDF().display()

# COMMAND ----------

df_delta = DeltaTable.forName(spark, 'target')

# COMMAND ----------



# COMMAND ----------

df_source = spark.createDataFrame([(1, "ram", 15), (4, "MM", 40), (5, "LL", 34)], ["id", "name", "age"])

# COMMAND ----------

df_source.createOrReplaceTempView('source')

# COMMAND ----------

target.alias('target').merge(df_source.alias('source'),"source.id=target.id").whenMatchedUpdate(set={'age':'source.age'}).whenNotMatchedInsert(values={'id':'source.id','name':'source.name','age':'source.age'}).execute()

# COMMAND ----------

df_delta.alias('target').merge(df_source.alias('source') , "source.id=target.id").whenMatchedUpdate(set = {'name': 'source.name', 'age':'source.age'}).whenNotMatchedInsertAll().execute()

# COMMAND ----------

DeltaTable.forName(spark, 'info').toDF().display()

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE into target
# MAGIC using source
# MAGIC on source.id=target.id
# MAGIC when matched then update set name=source.name, age=source.age
# MAGIC when not matched then insert (id,name,age) values(source.id,source.name,source.age)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from df_source
# MAGIC     
# MAGIC

# COMMAND ----------

# full load -- 100 mb , 200,300
# incremental load
#day 1 100mB
#day2 100mb
#day3 100mb

# COMMAND ----------

# MAGIC %md
# MAGIC ## scd type 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from target

# COMMAND ----------

'''
id  name age cur_rec_ind start_ts    end_ts
2	bb	20   Y            2025-07-23  null
3	cc	30   Y
1	aa	15   N             2025-07-23 2025-07-24
'''

# COMMAND ----------

'''1 aa 23 Y  2025-07-24  null
4 ff 45 Y
'''

# COMMAND ----------


spark.sql("""select id,name,age, 'Y' as cur_rec_ind , current_timestamp() as start_ts, 'Null' as end_ts from target """).write.mode("overwrite").option('mergeSchema',True).saveAsTable("target")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC update target set end_ts = Null where end_ts = 'Null'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from target

# COMMAND ----------

df_src = spark.createDataFrame([("3", "KK", 43), ("7", "UU", 48)], ["id", "name", "age"])

# COMMAND ----------

df_src.createOrReplaceTempView('src')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE into target
# MAGIC using src on src.id=target.id and target.cur_rec_ind ='Y'
# MAGIC when matched then update set cur_rec_ind = 'N'
# MAGIC , end_ts = current_timestamp()
# MAGIC when not matched then insert (id,name,age,cur_rec_ind,start_ts,end_ts) values(src.id,src.name,src.age,'Y',current_timestamp(),Null)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select src.* from src inner join target on src.id=target.id 
# MAGIC where target.cur_rec_ind='N'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into target(id,name,age,cur_rec_ind,start_ts,end_ts) 
# MAGIC select src.*,'Y' as cur_rec_ind,current_timestamp() as start_ts,Null as end_ts from src inner join target on src.id=target.id 
# MAGIC where target.cur_rec_ind='N'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from target where cur_rec_ind in ('Y','N')

# COMMAND ----------

# MAGIC %md
# MAGIC scd type 2

# COMMAND ----------

data=[(1,'jon',23,'Y','28-07-2025','null'),(2,'bob',24,'Y','28-07-2025','null'),(3,'carl',25,'Y','28-07-2025','null'),(4,'fred',26,'Y','28-07-2025','null'),(5,'gary',27,'Y','28-07-2025','null')]
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

schema=StructType([
    StructField('id', IntegerType()),
    StructField('name', StringType()),
    StructField('age', IntegerType()),
    StructField('cur_rec_ind', StringType()),
    StructField('start_ts', StringType()),
    StructField('end_ts', StringType())
])


# COMMAND ----------

data.display()

# COMMAND ----------

df_target=spark.createDataFrame(data,schema=(schema))

# COMMAND ----------

df_target.write.mode('overWrite').saveAsTable('workspace.default.target')

# COMMAND ----------

d_target1=DeltaTable.forName(spark,'target')

# COMMAND ----------

from delta.tables import DeltaTable

d_target1 = DeltaTable.forName(spark, 'target')

# COMMAND ----------

d_target1.toDF().display()

# COMMAND ----------

data=[(1,'jon',34,),(2,'bob',24),(3,'carl',52),(4,'fred',62),(6,'rose',23)]
schema=StructType([
    StructField('id', IntegerType()),
    StructField('name', StringType()),
    StructField('age', IntegerType())])

                                                                        
df_source=spark.createDataFrame(data,schema=(schema))


# COMMAND ----------

df_source.display()

# COMMAND ----------

req1=
matchiing record's cur_rec_ind should be updated to 'N' and end_ts should be updated to current date
non matching rrecord should be inserted as new record 1
req2=
matching latest  records from source should be inserted as new record  2
#write two merge statements for these two req;

# COMMAND ----------

#req1
d_target1.alias('target').merge(df_source.alias('source'),'target.id=source.id')\
    .whenMatchedUpdate(set={'cur_rec_ind':lit('N'),'end_ts':current_date()})\
        .whenNotMatchedInsert(values={'id':'source.id','name':'source.name','age':'source.age','cur_rec_ind':lit('Y'),'start_ts':current_date(),'end_ts':lit('null')}).execute()

# COMMAND ----------

d_target1.toDF().display()

# COMMAND ----------

#req 2
d_target1.alias('target').merge(df_source.alias('source'),'target.id=source.id and target.cur_rec_ind="Y"')\
    .whenNotMatchedInsert(values={'id':'source.id','name':'source.name','age':'source.age','cur_rec_ind':lit('Y'),'start_ts':current_date(),'end_ts':lit('null')}).execute()

# COMMAND ----------

from pyspark.sql.functions import*

# COMMAND ----------

d_target1.toDF().display()

# COMMAND ----------

#exceptional case 
exp1
wheren target table has already records on the same id
exp2
where same record tries to enter into target table which is already present

# COMMAND ----------

#new target data with duplicate records
data=[(1,'jon',23,'Y','28-07-2025','null'),(2,'bob',24,'Y','28-07-2025','null'),(3,'carl',25,'Y','28-07-2025','null'),(1,'jon',26,'N','28-07-2025','29-07-2025'),(2,'bob',27,'N','28-07-2025','29-07-2025')]
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

schema=StructType([
    StructField('id', IntegerType()),
    StructField('name', StringType()),
    StructField('age', IntegerType()),
    StructField('cur_rec_ind', StringType()),
    StructField('start_ts', StringType()),
    StructField('end_ts', StringType())
])
df_target=spark.createDataFrame(data,schema)

# COMMAND ----------

df_target.write.format('delta').saveAsTable('target10')

# COMMAND ----------

d_target2=DeltaTable.forName(spark,'target10')

# COMMAND ----------

d_target2.toDF().display()

# COMMAND ----------

data=[(1,'jon',34,),(2,'bob',24),(3,'carl',52),(4,'fred',62),(6,'rose',23)]
schema=StructType([
    StructField('id', IntegerType()),
    StructField('name', StringType()),
    StructField('age', IntegerType())])

                                                                        
df_source=spark.createDataFrame(data,schema=(schema))

# COMMAND ----------

df_source.display()


# COMMAND ----------

#exp 1
d_target2.alias('target').merge(df_source.alias('source'),'target.id=source.id and target.cur_rec_ind="Y"')\
.whenMatchedUpdate(set={'cur_rec_ind':lit('N'),'end_ts':current_date()})\
        .whenNotMatchedInsert(values={'id':'source.id','name':'source.name','age':'source.age','cur_rec_ind':lit('Y'),'start_ts':current_date(),'end_ts':lit('null')}).execute()

# COMMAND ----------

d_target2.toDF().display()

# COMMAND ----------

d_target2.alias('target').merge(df_source.alias('source'),'target.id=source.id and target.cur_rec_ind="Y"')\
    .whenNotMatchedInsert(values={'id':'source.id','name':'source.name','age':'source.age','cur_rec_ind':lit('Y'),'start_ts':current_date(),'end_ts':lit('null')}).execute()


# COMMAND ----------

d_target2.toDF().display()

# COMMAND ----------

#exp 2
data=[(1,'jon',23,'Y','28-07-2025','null'),(2,'bob',24,'Y','28-07-2025','null'),(3,'carl',25,'Y','28-07-2025','null'),(1,'jon',26,'N','28-07-2025','29-07-2025'),(2,'bob',27,'N','28-07-2025','29-07-2025')]
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

schema=StructType([
    StructField('id', IntegerType()),
    StructField('name', StringType()),
    StructField('age', IntegerType()),
    StructField('cur_rec_ind', StringType()),
    StructField('start_ts', StringType()),
    StructField('end_ts', StringType())
])
df_target=spark.createDataFrame(data,schema)

# COMMAND ----------

# DBTITLE 1,o
from pyspark.sql.functions import*
#to hNDLE EXP 2 GENERATE HASH KEY
df_target=df_target.withColumn('hash_key',md5(concat_ws('_',col('id'),col('name'),col('age'))))


# COMMAND ----------

df_target.write.format('delta').saveAsTable('target21')

# COMMAND ----------

from delta.tables import DeltaTable
d_target5=DeltaTable.forName(spark,'target21')

# COMMAND ----------

d_target5.toDF().display()

# COMMAND ----------

data=[(1,'jon',34,),(2,'bob',24),(3,'carl',52),(4,'fred',62),(6,'rose',23)]
schema=StructType([
    StructField('id', IntegerType()),
    StructField('name', StringType()),
    StructField('age', IntegerType())])

                                                                        
df_source=spark.createDataFrame(data,schema=(schema))

# COMMAND ----------

#adding hashkey to source
df_source=df_source.withColumn('hash_key',md5(concat_ws('_',col('id'),col('name'),col('age'))))



# COMMAND ----------

df_source.display()

# COMMAND ----------

d_target5.alias('target').merge(df_source.alias('source'),'target.id=source.id and target.cur_rec_ind="Y"').whenMatchedUpdate(condition=col('target.hash_key')!=col('source.hash_key'),set={'cur_rec_ind':lit('N'),'end_ts':current_date()})\
        .whenNotMatchedInsert(values={'id':'source.id','name':'source.name','age':'source.age','cur_rec_ind':lit('Y'),'start_ts':current_date(),'end_ts':lit('null'),'hash_key':'source.hash_key'}).execute()

# COMMAND ----------

d_target5.toDF().display()

# COMMAND ----------

d_target4.alias('target').merge(df_source.alias('source'),'target.id=source.id and target.cur_rec_ind="Y"')\
    .whenNotMatchedInsert(values={'id':'source.id','name':'source.name','age':'source.age','cur_rec_ind':lit('Y'),'start_ts':current_date(),'end_ts':lit('null'),'hash_key':'source.hash_key'}).execute()


# COMMAND ----------

d_target4.toDF().display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

d_target.alias('target').merge(df_source.alias('source'),'target.id=source.id').whenMatchedUpdate(set={'cur_rec_ind':lit('N'),'end_ts':current_date()}).whenNotMatchedInsertAll().execute()

# COMMAND ----------

d_target.toDF().display()

# COMMAND ----------

d_target.alias('target').merge(df_source.alias('source'),'target.id=source.id and target.cur_rec_ind="Y"').whenNotMatchedInsertAll().execute()

# COMMAND ----------

# DBTITLE 1,arget
d_target.toDF().display()

# COMMAND ----------

#to handle duplicate values
data=[(1,'jon',23,'N','28-07-2025','28-07-2025'),(1,'jon',23,'Y','28-07-2025','null'),(2,'bob',24,'Y','28-07-2025','null'),(3,'carl',25,'Y','28-07-2025','null'),(4,'fred',26,'Y','28-07-2025','null'),(5,'gary',27,'Y','28-07-2025','null')]
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

schema=StructType([
    StructField('id', IntegerType()),
    StructField('name', StringType()),
    StructField('age', IntegerType()),
    StructField('cur_rec_ind', StringType()),
    StructField('start_ts', StringType()),
    StructField('end_ts', StringType())
])


# COMMAND ----------

df_target=spark.createDataFrame(data,schema=(schema))
df_target=df_target.withColumn('hash_key',md5(concat_ws('_',col('id'),col('name'),col('age'))))

# COMMAND ----------

df_target.write.format('delta').mode('overWrite').saveAsTable('workspace.default.target2')

# COMMAND ----------

data=[(1,'jon',23,'Y','28-07-2025','null'),(7,'rose',24,'Y','28-07-2025','null'),(3,'carl',52,'Y','28-07-2025','null'),(4,'fred',62,'Y','28-07-2025','null'),(8,'gary',23,'Y','28-07-2025','null')]
schema=StructType([
    StructField('id', IntegerType()),
    StructField('name', StringType()),
    StructField('age', IntegerType()),
    StructField('cur_rec_ind', StringType()),
    StructField('start_ts', StringType()),
    StructField('end_ts', StringType())])
                                                                        
df_source=spark.createDataFrame(data,schema=(schema))



# COMMAND ----------

df_source=df_source.withColumn('hash_key',md5(concat_ws('_',col('id'),col('name'),col('age'))))

# COMMAND ----------

from delta.tables import *


# COMMAND ----------

d_target=DeltaTable.forName(spark,'workspace.default.target2')


# COMMAND ----------

df_source.display()

# COMMAND ----------

d_target.toDF().display()

# COMMAND ----------

d_target.alias('target').merge(df_source.alias('source'),'target.id=source.id  and target.cur_rec_ind="Y"').whenMatchedUpdate(condition='target.hash_key!=source.hash_key',set={'cur_rec_ind':lit('N'),'end_ts':current_date()}).whenNotMatchedInsertAll().execute()

# COMMAND ----------

d_target.toDF().display()

# COMMAND ----------

d_target.alias('target').merge(df_source.alias('source'),'target.id=source.id  and target.cur_rec_ind="Y"').whenNotMatchedInsertAll().execute()


# COMMAND ----------

d_target.toDF().display()

# COMMAND ----------

type 3 -  
target -- id name prev_age curr_age
source - id name age ----
################################################################
merge into target
using source on target.id=source.id
when matched then update set prev_age = curr_age, curr_age=source.age
when not matched then insert (id,name,prev_age,curr_age) values(source.id,source.name,source.age,source.age)

# COMMAND ----------

data=([(1,'ram','null','20'),(2,'raj','null','30'),(3,'ravi','null','40')])
schema=StructType([StructField('id',IntegerType()),StructField('name',StringType()),StructField('prev_age',StringType()),StructField('curr_age',StringType())])
df=spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

data=([(1,'ram',24),(2,'raj',34),(3,'ravi',40),(4,'rose',50)])
schema=StructType([StructField('id',IntegerType()),StructField('name',StringType()),StructField('age',IntegerType())])
df_source=spark.createDataFrame(data,schema)
df_source.display()

# COMMAND ----------

# we need to create hashkey so that no duplicate records are inserted
df=df.withColumn('hash_key',md5(concat(col('id'),col('name'),col('curr_age'))))
df.write.format('delta').mode('overwrite').saveAsTable('workspace.default.target6')
d_target6=DeltaTable.forName(spark,'target6')

# COMMAND ----------

#same for df_source
df_source=df_source.withColumn('hash_key',md5(concat(col('id'),col('name'),col('age'))))

# COMMAND ----------

d_target6.toDF().display()

# COMMAND ----------

d_target6.alias('target').merge(df_source.alias('source'),'target.id=source.id').whenMatchedUpdate(condition='target.hash_key!=source.hash_key',set={'prev_age':'curr_Age','curr_age':'source.age'}).whenNotMatchedInsert(values={'id':'source.id','name':'source.name','prev_age':lit("null"),'curr_age':'source.age','hash_key':'source.hash_key'}).execute()
d_target6.toDF().display()
