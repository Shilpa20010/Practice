# Databricks notebook source
# MAGIC %md
# MAGIC ##Merge two Dataframes using PySpark

# COMMAND ----------

simpleData = [(1,"Shilpa","CSE","UP",80,),\
    (2,"Shivam","IT","MH",90,),\
    (3,"Muni","EEE","RJ",70)]
columns = ['id','name','department','state','marks']
df1 = spark.createDataFrame(data= simpleData,schema= columns)
display(df1)

# COMMAND ----------

simpleData = [(5,"Shilpa","CSE","UP"),(6,"Shivam","IT","MH")]
columns = ['id','name','department','state']
df2 = spark.createDataFrame(data= simpleData,schema= columns)
display(df2)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

df2 = df2.withColumn("marks",lit("null"))
df = df1.union(df2)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Explode columns using PySpark

# COMMAND ----------

from pyspark.sql.functions import col,explode

simpledata = [(1,["Shilpa","Joshi"]),\
    (2,["Raj","Varma"]),\
    (3,["Arati","Sharma"]),\
    (4,["Kim"])]
columns = ["ID","Names"]

df = spark.createDataFrame(data = simpledata,schema = columns)
display(df)

# COMMAND ----------

df_output = df.select(col("ID"),explode(col("Names")))
display(df_output)

# COMMAND ----------


