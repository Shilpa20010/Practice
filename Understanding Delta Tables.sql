-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Understanding different concepts in Databricks
-- MAGIC

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC create database practice

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC
-- MAGIC create table practice.employee(id INT,name string,age int,salary double);

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC
-- MAGIC Insert into practice.employee 
-- MAGIC values
-- MAGIC (1, 'Shilpa', 21, 43000),
-- MAGIC (2, 'Raja', 17, 12000),
-- MAGIC (3, 'Sanvi', 32, 65000),
-- MAGIC (4, 'Ratan', 46, 32000);

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC
-- MAGIC select * from practice.employee;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC
-- MAGIC describe detail practice.employee 

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC
-- MAGIC describe history practice.employee

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC
-- MAGIC Update practice.employee
-- MAGIC set salary = 15000
-- MAGIC where id = 2;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC describe history practice.employee

-- COMMAND ----------

-- DBTITLE 1,Version history method1
-- MAGIC %sql
-- MAGIC
-- MAGIC select * from practice.employee version as of 1

-- COMMAND ----------

-- DBTITLE 1,Method2
-- MAGIC %sql
-- MAGIC
-- MAGIC select * from practice.employee@v1

-- COMMAND ----------

delete from practice.employee

-- COMMAND ----------

select * from practice.employee

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## How we restoret table data using version

-- COMMAND ----------

restore table practice.employee to version as of 2

-- COMMAND ----------

select * from practice.employee

-- COMMAND ----------

describe history practice.employee

-- COMMAND ----------

describe detail practice.employee

-- COMMAND ----------

Insert into practice.employee 
values
(9, 'Sanu', 29, 10000),
(10, 'Ravi', 25, 55000),
(11, 'Xender', 16, 5000),
(12, 'Pravin', 32, 35000),
(13, 'Aradhya', 20, 20000),
(14, 'saja', 16, 12000),
(15, 'ranvi', 52, 65000),
(16, 'gatan', 26, 38000);

-- COMMAND ----------

select * from practice.employee

-- COMMAND ----------

describe history practice.employee

-- COMMAND ----------

describe detail practice.employee

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Zorder command

-- COMMAND ----------


optimize practice.employee
zorder by (id)

-- COMMAND ----------

describe detail practice.employee

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/practice.db/employee'

-- COMMAND ----------

VACUUM practice.employee

-- COMMAND ----------


