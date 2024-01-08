-- Databricks notebook source
CREATE TABLE managed_default
(width INT, length INT, height INT)

-- COMMAND ----------

INSERT INTO managed_default
VALUES (3 INT , 2 INT, 1 INT)

-- COMMAND ----------

SELECT * FROM default.managed_default

-- COMMAND ----------

DESCRIBE EXTENDED managed_default

-- COMMAND ----------

create table external_default1
(width INT, length INT, height INT)
location 'dbfs:/demo/external_default1';

-- COMMAND ----------

INSERT INTO external_default1
VALUES(4 INT, 3 INT, 2 INT)

-- COMMAND ----------

DESCRIBE EXTENDED external_default1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##views

-- COMMAND ----------

create table smartphones
(id int, name string, brand string, year int);

-- COMMAND ----------

INSERT into smartphones
VALUES
(1 , 'iphone14','apple', 2021),
(2, 'iphone14plus','apple', 2022),
(3, 'vivo20','vivo',2022),
(4, 'redminote7','mi',2018),
(5, 'redminote9','mi',2020),
(6, 'galaxyj2','samsung',2022),
(7, 'galaxyj2next','samsung',2023);

-- COMMAND ----------

select * from default.smartphones

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #View

-- COMMAND ----------

create view view_apple_phone
as
select * from default.smartphones
where brand = 'apple';

-- COMMAND ----------

select * from view_apple_phone;

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Temporary View
-- MAGIC

-- COMMAND ----------

create temporary view temp_view_brand
as
select distinct brand from default.smartphones


-- COMMAND ----------

select * from temp_view_brand;

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Golbal_Temp_View

-- COMMAND ----------

create global temporary view gob_latestphone
as
select * from smartphones
where year > 2020
order by year desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## For selecting data from golbal view we need to select golbal_temp

-- COMMAND ----------

select * from global_temp.gob_latestphone;

-- COMMAND ----------

show tables

-- COMMAND ----------

show tables in global_temp;

-- COMMAND ----------


