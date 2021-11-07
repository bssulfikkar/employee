%md

## Overview

This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.

This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.


# File location and type
file_location = "/FileStore/tables/source1.json"
file_type = "json"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)
df_emp = df.distinct()
display(df_emp)


# Create a view or table

temp_table_name = "emp"

df_emp.createOrReplaceTempView(temp_table_name)



# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "tbl_emp"
df.write.format("parquet").saveAsTable(permanent_table_name)



# File location and type
file_location = "/FileStore/tables/source2.csv"
file_type = "csv"

schema = StructType([
            StructField("personal_number", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("position", StringType(), True),
            StructField("degree", StringType(), True),
            StructField("salary", DoubleType(), True),
            StructField("employee_number", IntegerType(), True)])

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .schema(schema)\
  .load(file_location)
df_salary = df.distinct()
display(df_salary)


# Create a view or table

temp_table_name = "emp_salary"

df_salary.createOrReplaceTempView(temp_table_name)


%sql
create table cleaned_emp_salary as
SELECT distinct
  personal_number,
  name,
  position,
    degree,
    salary,
    employee_number
FROM (
  SELECT
  personal_number,
    name,
  position,
    degree,
    salary,
    employee_number,
    dense_rank() OVER (PARTITION BY employee_number ORDER BY personal_number DESC) as rank
  FROM emp_salary order by employee_number) tmp
WHERE
  rank = 1

  
  df_cleaned_emp_salary = spark.sql("select * from cleaned_emp_salary")
  
  
  
  # With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "tbl_cleaned_emp_salary"
df_cleaned_emp_salary.write.format("parquet").saveAsTable(permanent_table_name)



from pyspark.sql.types import *

# File location and type
file_location = "/FileStore/tables/source3"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = " "

schema = StructType([
            StructField("personalnumber", IntegerType(), True),
            StructField("specialisation", StringType(), True),
            StructField("specialisation2", StringType(), True),
            StructField("specialisation3", StringType(), True),
            StructField("specialisation4", StringType(), True)])

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .schema(schema)\
  .load(file_location)

df_emp_qualification = df.distinct()

display(df_emp_qualification)


%sql
create table cleaned_emp_qualification as
select distinct personalnumber, specialisation as degree, concat(specialisation,' ', specialisation2, ' ',COALESCE(specialisation3, ''), ' ',COALESCE(specialisation4, '')) as specialisation from emp_qualification


df_cleaned_emp_qualification = spark.sql("select distinct * from cleaned_emp_qualification")


# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "tbl_cleaned_emp_qualification"
df_cleaned_emp_qualification.write.format("parquet").saveAsTable(permanent_table_name)



%sql
select max(tbl_cleaned_emp_salary.salary),tbl_cleaned_emp_qualification.degree  from tbl_emp
join tbl_cleaned_emp_qualification on
tbl_emp.personal_number = tbl_cleaned_emp_qualification.personalnumber
join tbl_cleaned_emp_salary on
tbl_cleaned_emp_qualification.degree= tbl_cleaned_emp_salary.degree
group by tbl_cleaned_emp_qualification.degree 


%sql
select max(tbl_cleaned_emp_salary.salary),tbl_cleaned_emp_salary.position, tbl_cleaned_emp_qualification.degree  from tbl_emp
join tbl_cleaned_emp_qualification on
tbl_emp.personal_number = tbl_cleaned_emp_qualification.personalnumber
join tbl_cleaned_emp_salary on
tbl_cleaned_emp_qualification.degree= tbl_cleaned_emp_salary.degree
group by tbl_cleaned_emp_salary.position , tbl_cleaned_emp_qualification.degree
