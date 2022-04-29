import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
#import great_expectations as ge
#import html_to_json

import spark_df_profiling 
spark = SparkSession.builder.master("local").appName("sample").getOrCreate()
print(spark)
##df = spark\
#.read\
#.format("csv")\
#.options(header = 'true', inferSchema = 'true')\
#.load("/home/jayyu1010/dbtwork/data/yellow_tripdata_sample_2019-01.csv")
#df.createOrReplaceTempView('taxi')
#print(df.count())

df = spark\
.read\
.format("csv")\
.options(header = 'true', inferSchema = 'true', sep='\t')\
.load("/home/jayyu1010/dbtwork/data/amazon_reviews_us_Camera_v1_00.tsv.gz")

columnList = [item[0] for item in df.dtypes if item[1].startswith('string')]

df = df[columnList]

#df = df.select(col('review_date').alias('review_date'),col('star_rating').alias('rating'),col('marketplace').alias('mp'))
#df = df.withColumn('year', year(col('review_date')))

#ds = ge.dataset.SparkDFDataset(spark.table('taxi'))
#er = ds.expect_column_values_to_be_between('passenger_count',1,5)

profile = spark_df_profiling.ProfileReport(df,minimal=True,check_correlation = False)
#print(profile)
#print(type(profile))
profile.to_file(outputfile="myoutputfile.html")
#profile.to_csv('test.csv')
#print(er.result)
