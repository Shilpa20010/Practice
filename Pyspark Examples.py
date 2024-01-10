# Databricks notebook source
# MAGIC %md
# MAGIC ##Remove double quotes from value of json string using PySpark

# COMMAND ----------

jsonstring = '{"ID":"2","Name":"SHILPA","City":"Hyd"}'
data = [(1,'{"ID":"1","Name":"SHIVAM"JAVED,"City":"Mumbai"}'),(2,jsonstring)]

cols = ["col1","col2"]
df = spark.createDataFrame(data,cols)
display(df)

# COMMAND ----------

from pyspark.sql.functions import split, col,lit,concat_ws,concat

# COMMAND ----------


df = df.withColumn("col3",split(col("col2"),'"Name":"')[0])\
    .withColumn("col4",lit('"Name":"'))\
    .withColumn("col5",split(col("col2"),'"Name":"')[1])
display(df)

# COMMAND ----------

df = df.withColumn("col6",split(col("col5"),'"',2))
df = df.withColumn("col7",concat_ws('',col("col6")))
df = df.withColumn("col8",concat(col("col3"),col("col4"),col("col7"))).select(col("col2"),col("col8"))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Check if a Column exists in DataFrame using PySpark

# COMMAND ----------

data = [(1,"Shilpa","Female",20000),(2,"Raj","Male",15000)]
column = ['id','name','gender','salary']

df = spark.createDataFrame(data,column)
display(df)

# COMMAND ----------

print(df.schema)

# COMMAND ----------

print(df.schema.fieldNames())

# COMMAND ----------

from pyspark.sql.functions import count

# COMMAND ----------

fieldnames = df.schema.fieldNames()
if fieldnames.count('id')>0:
    print('id column is present')
else:
    print('id column is not present')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Convert PySpark Dataframe to Pandas Dataframe

# COMMAND ----------

data = [(1,"Shilpa","Female",20000),(2,"Raj","Male",15000)]
column = ['id','name','gender','salary']

df = spark.createDataFrame(data,column)
display(df)
print(type(df))

# COMMAND ----------

df = df.toPandas()
print(df)
print(type(df))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Different ways to apply function on Column in Dataframe using PySpark

# COMMAND ----------

data = [(1,'shilpa'),(2,'reva')]
columns = ['id','name']

df = spark.createDataFrame(data,columns)
display(df)

# COMMAND ----------

from pyspark.sql.functions import upper

df.withColumn('name',upper(df.name)).show()

# COMMAND ----------

df.select('id', upper(df.name).alias('name')).show()

# COMMAND ----------

from pyspark.sql.functions import upper

# COMMAND ----------

def uppername(df):
    return df.withColumn('name',upper(df.name))

df.transform(uppername).show()

# COMMAND ----------


