from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("BigDataAnalysis").getOrCreate()
from pyspark.sql import functions as F
from pyspark.sql.functions import split,size,col,regexp_replace
import re
df = spark.read.csv('student_dataset.csv',header=True,inferSchema=True)

# Rename the column 'Phone_No.' to 'Phone_No'
df = df.withColumnRenamed('Phone_No.', 'Phone No')
# Rename the column 'Phone_No.' to 'Phone_No'
df = df.withColumnRenamed(' Student Names',' Student_Names')
# Rename the column 'Roll No.' to 'Roll_No'
df = df.withColumnRenamed('Roll No.', 'Roll_No')

df.printSchema()

df.show(5)

df.describe().show()

df.select('Student_Names').distinct().show()

filtered_df = df.filter(df['Chemistry']>5)

average_value = filtered_df.agg(F.avg("Math")).collect()[0][0]
print(f"Average value:{average_value}")

df = df.withColumn("Comment",regexp_replace(col("Comment"),"[^\\w\\s]",""))

words = df.withColumn("word",split(col("Comment"),"\\s+"))

spark.stop()