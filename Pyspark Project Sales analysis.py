# Databricks notebook source
# DBTITLE 1,sales df
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

schema = StructType([StructField("product_id",IntegerType(),True),StructField("customer_id",StringType(),True),StructField("order_date",DateType(),True),StructField("location",StringType(),True),StructField("source_order",StringType(),True)])

# COMMAND ----------

sales_df = spark.read.format("csv").option("inferschema",True).schema(schema).load("/FileStore/shexplode/Shilpa/sales_csv.txt")
display(sales_df)

# COMMAND ----------

# DBTITLE 1,Deriving Year, month and quarter
from pyspark.sql.functions  import month,year,quarter

sales_df = sales_df.withColumn('Order_year',year(sales_df.order_date))
sales_df = sales_df.withColumn('Order_month',month(sales_df.order_date))
sales_df = sales_df.withColumn('Order_quarter',quarter(sales_df.order_date))

# COMMAND ----------

display(sales_df)

# COMMAND ----------

# DBTITLE 1,Menu DF
from pyspark.sql.types import StructField, StringType,IntegerType,StringType,DateType

schema = StructType([StructField("product_id",IntegerType(),True),StructField('product_name',StringType(),True),StructField('price',StringType(),True)])

menu_df = spark.read.format('csv').option('inferschema',True).schema(schema).load('/FileStore/shexplode/Shilpa/menu_csv.txt')

# COMMAND ----------

display(menu_df)

# COMMAND ----------

# DBTITLE 1,Total amount spend by each customer
total_amount_spent = (sales_df.join(menu_df,on='product_id').groupBy('customer_id').agg(sum('price').alias('Total_amount')).orderBy('customer_id'))

display(total_amount_spent)

# COMMAND ----------

# DBTITLE 1,Total amount spent by each food category
total_amount_spent_food = (sales_df.join(menu_df,on='product_id').groupBy('product_name').agg(sum('price').alias('Total_amount_food')).orderBy('product_name'))

display(total_amount_spent_food)

# COMMAND ----------

# DBTITLE 1,Total amount of sales in each month
total_amount_month = (sales_df.join(menu_df,on='product_id').groupBy('Order_month').agg(sum('price').alias('Total_month')).orderBy('Order_month'))
display(total_amount_month)

# COMMAND ----------

# DBTITLE 1,Yearly sales
total_amount_year = (sales_df.join(menu_df,on='product_id').groupBy('Order_year').agg(sum('price')).orderBy('Order_year'))
display(total_amount_year)

# COMMAND ----------

# DBTITLE 1,Quarterly sales
total_amount_quarter = (sales_df.join(menu_df,on='product_id').groupBy('Order_quarter').agg(sum('price').alias('Total_quarter_amount')).orderBy('Order_quarter'))
display(total_amount_quarter)

# COMMAND ----------

# DBTITLE 1,Total number of order by each category
from pyspark.sql.functions import count

most_count_df = (sales_df.join(menu_df,on='product_id').groupBy('product_id','product_name').agg(count('product_id').alias('product_count')).orderBy('product_count',ascending = 0).drop('product_id'))

display(most_count_df)

# COMMAND ----------

# DBTITLE 1,Top 5 Ordered items
from pyspark.sql.functions import count

most_5order_df = (sales_df.join(menu_df,on='product_id').groupBy('product_id','product_name').agg(count('product_id').alias('product_count')).orderBy('product_count',ascending = 0).drop('product_id').limit(5))

display(most_5order_df)

# COMMAND ----------

# DBTITLE 1,Top order
from pyspark.sql.functions import count

most_order_df = (sales_df.join(menu_df,on='product_id').groupBy('product_id','product_name').agg(count('product_id').alias('product_count')).orderBy('product_count',ascending = 0).drop('product_id').limit(1))

display(most_order_df)

# COMMAND ----------

# DBTITLE 1,Frequency of customer visited
from pyspark.sql.functions import countDistinct

df_frequently = sales_df.filter(sales_df.source_order=='Restaurant').groupBy('customer_id').agg(countDistinct('order_date'))
display(df_frequently)

# COMMAND ----------

# DBTITLE 1,Total sales by each country
total_sales_df = sales_df.join(menu_df,on='product_id').groupBy('location').agg(sum('price'))
display(total_sales_df)

# COMMAND ----------

# DBTITLE 1,total sales by source order
source_df = sales_df.join(menu_df,on='product_id').groupBy('source_order').agg(sum('price')).orderBy('source_order')
display(source_df)

# COMMAND ----------


