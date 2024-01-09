# Databricks notebook source
from pyspark.sql.functions import collect_list,collect_set, udf
from pyspark.sql.types import IntegerType

# COMMAND ----------

data = [('James','Java'),
  ('James','Python'),
  ('James','Python'),
  ('Anna','PHP'),
  ('Anna','Javascript'),
  ('Maria','Java'),
  ('Maria','C++'),
  ('James','Scala'),
  ('Anna','PHP'),
  ('Anna','HTML')
]

df = spark.createDataFrame(data,schema=["name","languages"])
df.printSchema()
df.show()

# COMMAND ----------

df2 = df.groupBy("name").agg(collect_list("languages") \
    .alias("languages"))
df2.printSchema()    
df2.show()

# COMMAND ----------

df2 = df.groupBy("name").agg(collect_set("languages") \
    .alias("languages"))
df2.printSchema()    
df2.show()

# COMMAND ----------

data = [(1,'Shilpa',2000,500),(2,'Raj',3000,200)]
schema = ['id','name','salary','bonus']

df = spark.createDataFrame(data, schema)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##1st method to write UDF

# COMMAND ----------

def total_pay(a,b):
    return a+b

Totalpay = udf(lambda a,b:total_pay(a,b),IntegerType())

# COMMAND ----------

df.withColumn('TotalPay',Totalpay(df.salary,df.bonus)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Second method to write UDF

# COMMAND ----------

@udf(returnType=IntegerType())
def total_pay(a,b):
    return a+b

# COMMAND ----------

df.select('*', total_pay(df.salary,df.bonus).alias('Totpay')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register UDF and use it with spark sql

# COMMAND ----------

data = [(1,'Shilpa',2000,500),(2,'Raj',3000,200)]
schema = ['id','name','salary','bonus']

df = spark.createDataFrame(data, schema)
df.createOrReplaceTempView('emps')

# COMMAND ----------

def total_pay(a,b):
    return a+b

# COMMAND ----------

spark.udf.register(name='TotalPay',f=total_pay,returnType=IntegerType())

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select id,name, TotalPay(salary,bonus) as Totalpay from emps

# COMMAND ----------


