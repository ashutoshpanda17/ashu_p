# Databricks notebook source
# MAGIC %md
# MAGIC # SCD(Slow Changing Dimension)
# MAGIC
# MAGIC **Definition:**
# MAGIC
# MAGIC This is a techniques which is used in data warehousing and modern data platforms (like Delta Lake) to manage and track changes in dimension data(descriptive information for e.g., Customer dimension: customer name, city, age) over time (e.g., customer details, employee info).

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# COMMAND ----------

scd type 1--only keep current record or latest record
scd type 2-- it keeps current record and history record
scd type 3-- keeps only current and previous record


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists dlt_cust_target;

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC
# MAGIC %md
# MAGIC
# MAGIC ### Before we go deeper into SCD Type 1 and 2, let’s understand why we even need these techniques.
# MAGIC
# MAGIC **Lets Imagine**
# MAGIC
# MAGIC today your dimension table is 100 MB in size.
# MAGIC Tomorrow, it grows to 200 MB next day 300MB. If you reload the entire table every day, that’s called a full load — and it’s not efficient. It consumes time, storage, and processing so performance it is not good
# MAGIC
# MAGIC **Instead we prefer Incremental Load**
# MAGIC
# MAGIC Instead, we prefer an incremental load  where only new or changed records are processed or appended.
# MAGIC That’s where SCD Type 1 and Type 2 came into picture which let us to load only the updated data or new data instead of reloading the whole data so this bascially ahppens due to **MERGE** logic.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # SCD Type 1
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC  **SCD Type 1:**
# MAGIC  
# MAGIC  - SCD Type 1 is used when we only need the latest data — it overwrites the old value with the new one and does not maintain any history.
# MAGIC
# MAGIC **Use Case:** 
# MAGIC
# MAGIC - When data corrections are needed (e.g., fixing typos).
# MAGIC
# MAGIC **Example:**
# MAGIC
# MAGIC  - If a customer's name was wrongly saved as "Raj" but his orginal name is "Raja" then Type 1 simply just updates it to "Raja" but no record of "Raj" is kept.
# MAGIC
# MAGIC **History:** 
# MAGIC
# MAGIC - No history maintained.
# MAGIC

# COMMAND ----------

# Example: for SCD1
# ===================

# lets create a DataFrame : -
# ===========================
data = [(101,'Rahul',"Bam",45),(102,'Raja','Ctp',28),(103,'Shayam',"Aska",19),
        (104,"Sanket","Humma",58)]

schema = StructType([StructField('id',IntegerType(),True),StructField('name',StringType(),True),StructField('Address',StringType(),True),StructField('Age',IntegerType(),True)])

df_cust_target = spark.createDataFrame(data,schema)
df_cust_target.display()


# COMMAND ----------

# scenario : as we know our target table is df_cust_target suppose we recive a source file where new record is like this
# id  name    Add   age
# 101 sanket  Ctp   61

# so now we have to update it in the target table

# for understanding now lets create a source table with this new record

data = [(104,'Sanket','Ctp',61),(109,'Sudip',"Bam",28),(108,'Boon','Ctp',24)]
schema = StructType([StructField('id',IntegerType(),True),StructField('name',StringType(),True),StructField('Address',StringType(),True),StructField('Age',IntegerType(),True)])

df_cust_source = spark.createDataFrame(data,schema)
df_cust_source.display()

# COMMAND ----------

# lets merge the target table with the source table

df_cust_target.merge(
    "df_cust_source", "df_cust_target.id = df_cust_source.id"
).whenMatchedUpdate(
    set={
        "id": "df_cust_source.id",
        "name": "df_cust_source.name",
        "Address": "df_cust_source.Address",
        "Age": "df_cust_source.Age",
    }
).whenNotMatchInsertAll().execute()  #Error: Attribute `merge` is not supported(as we have seen merge is not supporting for the DataFrame so in DataBricks the Target table must be Delta table

#Note:  Target table must be Delta when we are merging the data
#        Source table can be DataFrame,Csv,Or any file system

# COMMAND ----------

# lets convert target table data into delta table
df_cust_target.write.format('Delta').mode('overwrite').saveAsTable('dlt_cust_target')  #got converted

# lets check delta table created or not
DeltaTable.forName(spark,'dlt_cust_target').toDF().display() # yes it is created

# lets create a refrence for the DeltaTable to reuse it like an alias
df_target = DeltaTable.forName(spark,'dlt_cust_target')
df_target.toDF().display()


# COMMAND ----------

# lets perfrom the merge statement

df_target.merge(df_cust_source,'df_target.id=df_cust_source.id')\
.whenMatchedUpdate(set={'id':'df_cust_source.id','name':'df_cust_source.name',
                      'Address':'df_cust_source.Address',
                      'Age':'df_cust_source.Age'})\
.whenNotMatchedInsert(values={'id':'df_cust_source.id','name':'df_cust_source.name',
                             'Address':'df_cust_source.Address',
                             'Age':'df_cust_source.Age'}).execute()    


# so basically this error is due to the alias like we used to give in merge statement this is ambiguity like age is there in both the column

# COMMAND ----------

# by giving the alias it will work
df_target.alias('target').merge(df_cust_source.alias('source'),'target.id=source.id')\
    .whenMatchedUpdate(set={'id':'source.id',
                           'name':'source.name',
                           'Address':'source.Address',
                           'Age':'source.Age'})\
    .whenNotMatchedInsert(values={'id':'source.id','name':'source.name',
                             'Address':'source.Address',
                             'Age':'source.Age'}).execute()
    
    # we have some option like if we want to insert all the Non-matching data from source table then we can use whenNotMatchedInsertAll() or for specific column we can use whenNotMatchedInsert()... and remember one thing the operator we use must be similar to sql like('==' can be writen as '='  , '&' we can write like SQL 'and' or etc)

# COMMAND ----------

# lets see the target table
spark.table('dlt_cust_target').display()   

# see what happen here only the new record is inserted and the old record is updated but no history record is created so this is called scd type1

# COMMAND ----------

#using SQL
df_cust_source.createOrReplaceTempView('df_source')


# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO dlt_cust_target AS df_t  --in sql we cant give the reference of the delta table (X df_target)
# MAGIC USING df_source AS df_s
# MAGIC ON df_t.id = df_s.id
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET df_t.Address = df_s.Address
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT(id,name,Address,Age) VALUES(df_s.id,df_s.name,df_s.Address,df_s.Age);
# MAGIC
# MAGIC -- using sql merge statement is the beinfit like we can see num_affected_rows,update rows which is not possible in pyspark merge statement

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dlt_cust_target;

# COMMAND ----------

# MAGIC %md
# MAGIC # Upsert 
# MAGIC
# MAGIC **Definition:**
# MAGIC
# MAGIC Upsert is a combination of the words Update and Insert.
# MAGIC
# MAGIC **It means:**
# MAGIC
# MAGIC -  Update the record if it already exists
# MAGIC
# MAGIC -  Insert a new record if it does not exist
# MAGIC
# MAGIC -  This ensures that duplicate data is avoided and existing records stay current.
# MAGIC
# MAGIC **How Upsert Works:**
# MAGIC
# MAGIC -  WHEN MATCHED → Perform an UPDATE
# MAGIC
# MAGIC -  WHEN NOT MATCHED → Perform an INSERT
# MAGIC
# MAGIC This logic is commonly implemented using the MERGE statement in SQL or tools like Delta Lake, Databricks, and Snowflake.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # SCD type 2
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **SCD Type 2:** 
# MAGIC - SCD Type 2 is a data warehousing method used to track historical changes by inserting a new row each time the data changes.
# MAGIC
# MAGIC - Each version of the data is saved with a start date, end date, and a flag like is_current to show which row is the latest.
# MAGIC
# MAGIC **Why SCD Type 2 is used?**
# MAGIC - The limitation in SCD Type 1 is it only updates the existing record, so it does not store any history.which becomes a problem when we need to track past data for reports, audits, or time-based analysis.
# MAGIC
# MAGIC - That’s why we use SCD Type 2, which keeps both old and new records with a "start date", "end date", and "is_active"(current record indicator,active flag indicator there is no fixed name this may vary project to project) 
# MAGIC
# MAGIC **Use Case:**
# MAGIC - When we need to keep full history of changes — like tracking a customer’s address change, employee department transfer, or product price updates — so we can see what the data was at the previous what data was update currently.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# suppose lets take a scenario the "sanket"&"sudip" from "ctp" moved to "amabapua" after some months but i want there must be previous record with the old record but if i do so i can achieve using append but i cant indicate which wasa previous "Address" and which is now "Address" so for that we can use the "curr_indicator","Current" & "End" TimeStamp()

# lets assume after some days i got a new source file
schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('Address', StringType(), True),
    StructField('Age', IntegerType(), True)
])

df_cust_source1 = spark.createDataFrame(
    [
        (104, 'Sanket', 'Ambapua', 61),
        (109, 'Sudip', 'Ambapua', 28),
        (118, 'Bhanu', 'TataColony', 44),
        (105, 'Ravi', 'Bam', 28)
    ],
    schema=schema
)
df_cust_source1.display()

# COMMAND ----------

# now i have to merge with the the target table but i want to keep the history of the data so i will use scd type 2(keep both records) using insert for second time

#before that lets update the target table with curr_rec indicator,timestamp(start,end)

df = spark.sql("""select id,name,Address,age,'Y' as curr_rec_ind,'27-04-2025' as start_ts ,'null' as end_ts  from dlt_cust_target""")

#lets re write to the delta table
df.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable('dlt_cust_target')
DeltaTable.forName(spark,'dlt_cust_target').toDF().display()

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE dlt_cust_target SET end_ts = Null where end_ts = 'null';

# COMMAND ----------

df_target1 = DeltaTable.forName(spark,'dlt_cust_target')
df_target1.toDF().display()

# COMMAND ----------

# as we got source data today which was mentioned before lets merge that
#req1: if the data is present in source table and then update the current record indicator to 'N' and end_ts to current date and if not there then insert the new record and set the current record indicator to 'Y' and start_ts to current date

from pyspark.sql.functions import current_date, lit

df_target1.alias('target') \
.merge(
    df_cust_source1.alias('source'),
    "target.id = source.id AND target.curr_rec_ind = 'Y'"  #always filter 'Y' bcoz 'N' is not required to compare as it is old record
) \
.whenMatchedUpdate(set={
    'curr_rec_ind': lit('N'),
    'end_ts': current_date()
}) \
.whenNotMatchedInsert(values={
    'id': 'source.id',
    'name': 'source.name',
    'Address': 'source.Address',
    'age': 'source.Age',
    'curr_rec_ind': lit('Y'),
    'start_ts': current_date(),
    'end_ts': lit(None)  # Don't use 'null' as string, use actual null with lit(None)
}).execute()


# COMMAND ----------

df_target1.toDF().display()

# COMMAND ----------

# req2: as we saw that sanket & sudip currentIndicator is 'N' so i want to insert the new record with the new address and make curr_rec_ind to 'Y' and start_ts to current date and end_ts to null

df_target1.alias('target') \
.merge(df_cust_source1.alias('source'),"target.id = source.id and target.curr_rec_ind = 'Y'").whenNotMatchedInsert(values={
    'id': 'source.id',
    'name': 'source.name',
    'Address': 'source.Address',
    'age': 'source.Age',
    'curr_rec_ind': lit('Y'),
    'start_ts': current_date(),
    'end_ts': lit(None)
}).execute()
df_target1.toDF().display()

# COMMAND ----------

# suppose lets take another scenario that a new source file came but that source file having dupllicate record which was already present in the target table so we have to ignore that record and insert the new record


schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('Address', StringType(), True),
    StructField('Age', IntegerType(), True)
])

df_cust_source2 = spark.createDataFrame(
    [
        (109, 'Sudip', 'Ambapua', 28),
        (114, 'abhi', 'chilika', 31),
        (117, 'linga', 'kahlikote',40)],schema = schema)
df_cust_source2.display()


# COMMAND ----------

df_target1.alias('target').merge(df_cust_source2.alias('source'),"target.id=source.id and target.curr_rec_ind='Y'").whenMatchedUpdate(set={'curr_rec_ind':lit('N'),'end_ts':current_date()}).whenNotMatchedInsert(values={'id':'source.id','name':'source.name','Address':'source.Address','age':'source.Age','curr_rec_ind':lit('Y'),'start_ts':current_date(),'end_ts':lit(None)}).execute()
df_target1.toDF().display()

# COMMAND ----------

df_target1.alias('target') \
.merge(df_cust_source1.alias('source'),"target.id = source.id and target.curr_rec_ind = 'Y'").whenNotMatchedInsert(values={
    'id': 'source.id',
    'name': 'source.name',
    'Address': 'source.Address',
    'age': 'source.Age',
    'curr_rec_ind': lit('Y'),
    'start_ts': current_date(),
    'end_ts': lit(None)
}).execute()
df_target1 = DeltaTable.forName(spark,'dlt_cust_target')
df_target1.toDF().display()


# COMMAND ----------

# now see the duplicated record got inserted which is bad and the current record which was y got n
# for that reason we can use haskey to find the duplicate rows in source table and handle them by skipping before merge into target table

#let generate hash key for both the table now

df_target_hash = df_target1.toDF().withColumn('hash_key',md5(concat_ws('-',col('id'),col('name'),col('Address'),col('age')) ))
df_target_hash.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable('dlt_cust_target')

# COMMAND ----------

# now we have to generate the hash key for source column so that we will match if any duplicate we will skip it beofre updating / inserting

df_source_hash = df_cust_source2.withColumn('hash_key',md5(concat_ws('-',col('id'),col('name'),col('Address'),col('Age'))))
df_source_hash.display()

# COMMAND ----------

df_target_hash = DeltaTable.forName(spark,'dlt_cust_target')
df_target_hash.toDF().display()

# COMMAND ----------

# now lets merge them with one conditon that if hash keys are matched then dont insert the record

df_target_hash.alias("target").merge(
    df_source_hash.alias("source"), 'target.id=source.id and target.curr_rec_ind="Y" and target.hash_key!=source.hash_key').whenMatchedUpdate(set={'curr_rec_ind':lit('N'),'end_ts':current_date()}).whenNotMatchedInsert(values={'id':'source.id','name':'source.name','Address':'source.Address','age':'source.Age','curr_rec_ind':lit('Y'),'start_ts':current_date(),'end_ts':lit(None),'hash_key':'source.hash_key'}).execute()
df_target_hash.toDF().display()  

# COMMAND ----------

#lets insert the record in the target table
df_target_hash.alias("target").merge(
    df_source_hash.alias("source"), 'target.id=source.id and target.curr_rec_ind="Y"').whenNotMatchedInsert(values={'id':'source.id','name':'source.name','Address':'source.Address','age':'source.Age','curr_rec_ind':lit('Y'),'start_ts':current_date(),'end_ts':lit(None),'hash_key':'source.hash_key'}).execute()
df_target_hash.toDF().display()

# COMMAND ----------

#no it didnt got handled as we have used the hash key in merge condition so before matching it filterning out the duplicate record and as the hash key are same so the condition is not matching so it is inserting the reocrd so for that we can use condition inside when match update

df_target_hash.alias("target").merge(
    df_source_hash.alias("source"), 'target.id=source.id and target.curr_rec_ind="Y"').whenMatchedUpdate(condition='target.hash_key!=source.hash_key',set={'curr_rec_ind':lit('N'),'end_ts':current_date()}).whenNotMatchedInsert(values={'id':'source.id','name':'source.name','Address':'source.Address','age':'source.Age','curr_rec_ind':lit('Y'),'start_ts':current_date(),'end_ts':lit(None),'hash_key':'source.hash_key'}).execute()
df_target_hash.toDF().display()   #see the duplicate record got handled

#so what it does it first filterd the id which are matching then i have took tohse are 'Y' after that it will match and moved inside when matched update and there i have put the condition of hash key that if not matching then only set N otherwise dont do anything

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("Address", StringType(), True),
    StructField("Age", IntegerType(), True)
])

df_source = spark.createDataFrame([
    (104, "Sanket", "Ambapua", 61),
    (109, "Sudip", "Ambapua", 28),
    (118, "Bhanu", "TataColony", 44),
    (105, "Ravi", "Bam", 28)
], schema=schema)


# COMMAND ----------



# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from datetime import date

target_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("Address", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("curr_rec_ind", StringType(), True),
    StructField("start_ts", DateType(), True),
    StructField("end_ts", DateType(), True)])

df_target = spark.createDataFrame([
    (104, "Sanket", "CTP", 61, "N", date(2024, 1, 1), date(2025, 7, 24)),
    (104, "Sanket", "Ambapua", 61, "Y", date(2025, 7, 24), None),
    (109, "Sudip", "CTP", 28, "N", date(2024, 1, 1), date(2025, 7, 24)),
    (109, "Sudip", "Ambapua", 28, "Y", date(2025, 7, 24), None),
    (118, "Bhanu", "TataColony", 44, "Y", date(2025, 7, 24), None),
    (105, "Ravi", "Bam", 28, "Y", date(2025, 7, 24), None)
], schema=target_schema)


# COMMAND ----------

df_cust_target.display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


