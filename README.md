# taxRegister
Convert SQL queries to PySpark for efficient data processing and analysis
INTRODUCTION TO PYSPARK

Pyspark
PySpark is the Python API for Apache Spark, an open-source, distributed computing system used for big data processing and analytics.
HDFS
HDFS, or Hadoop Distributed File System, is the primary storage system used by Apache Hadoop, a framework for distributed storage and processing of large data sets
Dataframe
DataFrames are distributed data collections arranged into rows and columns in PySpark.

                              Reading a csv into a dataframe
df= spark.read.csv("/BFIX1/JOB/COM.GEN_T_Document_Approvals", header='True')

                               Joining Operation 
SQL

select t1.column1, t2.column2 
from table1 t1 inner join table2 t2 
on t1.key1 = t2.key2;

Pyspark
result = df1.join(df2, df1.key == df2.key,”inner”).select("t1.column1", "t2.column2")



                             Filtering with WHERE Clause
SQL
select * from orders where order_date >= '2022-01-01';
Pyspark
filtered_orders_df = orders_df.filter(col("order_date") >= "2022-01-01")
                       
 Order by
SQL
select customer_id, first_name, last_name
from customers
where age >= 18
order by last_name asc;
Pyspark
filtered_sorted_customers_df = customers_df.select("customer_id", "first_name", "last_name") \
    .filter(customers_df.age >= 18) \
    .orderBy(col("last_name").asc())
                         Grouping and filtering with a having clause
SQL
select category, count(*) as count
from products
group by category
having count(*) > 10;
Pyspark
aggregated_filtered_products_df = products_df.groupBy("category") \
    .agg(count("*").alias("count")) \
    .filter(col("count") > 10)
                              Union operation
df_union =df1.union(df2)

                              Window Functions:
SQL
select column1, column2, avg(column2) over (partition by column1 order by column3) as avg_col2 from table;
Pyspark
from pyspark.sql.window import Window

window_spec = Window().partitionBy("column1").orderBy("column3")
result = df.select("column1", "column2", avg("column2").over(window_spec).alias("avg_col2"))

                            CASE WHEN statement:
SQL
select column1, 
       case when column2 > 100 then 'High' 
            when column2 > 50 then 'Medium' 
            else 'Low' end as category  from table;
Pyspark
from pyspark.sql.functions import when
result = df.select("column1", 
                   when(df.column2 > 100, 'High')
                   .when(df.column2 > 50, 'Medium')
                   .otherwise('Low')
                   .alias("category"))

                              String Operation
SQL
select column1, concat(column2, ' - ', column3) as combined_str from table;
Pyspark
from pyspark.sql.functions import concat, lit
result = df.select("column1", concat(df.column2, lit(' - '), df.column3).alias("combined_str"))

                           Distinct Values
SQL
select distinct column1 from table;
Pyspark
result = df.select("column1").distinct()

                            


                      Substring Operation:
SQL
select column1, substring(column2, 1, 3) as substr_col2 from table;
Pyspark
from pyspark.sql.functions import substring
result = df.select("column1", substring(df.column2, 1, 3).alias("substr_col2"))

                      Converting Data Types:
SQL
select column1, cast(column2 as int) as int_col from table;
Pyspark
result = df.select("column1", df.column2.cast("int").alias("int_col"))

                         delete dataframe (read.csv) at end
del df

DropDuplicates:
df_duplicates = df.dropDuplicates(‘column1’,’column2’,’column3’)



When, isnull, otherwise:
 df_s = df.withColumn("status", when(col("age").isNull(), "Unknown").otherwise(when(col("age") < 18, "Minor").otherwise("Adult")))

Sum, Avg, Max, Min:
df_status = df.select(sum(col("age")),avg(col("age")),
                   min(col("age")), max(col("age")))

WithColumn 
df_2=df_1.withColumn("new_name",df_up1.column)

 






