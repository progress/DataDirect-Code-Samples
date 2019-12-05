
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
 
 
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
 
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
 
##Read Data from REST API using DataDirect Autonomous REST Connector JDBC driver in to DataFrame
source_df = spark.read.format("jdbc").option("url","jdbc:datadirect:autorest:config=yelp.rest;AuthenticationMethod=HttpHeader;AuthHeader=Authorization;SecurityToken='Bearer JcMUtuWfaqJdWJBqqLrgBxfbYh6GIUGv3zUyXOG4zsfe6wnOtlZBeroFb8rpRM-dESFzcSAUd1YDAtQm2yl0hrJwfldvHp2AdEzRXThZku69r-w4wTv80Cj7d08ZXHYx'").option("dbtable", "AUTOREST.BUSINESSES").option("driver", "com.ddtek.jdbc.autorest.AutoRESTDriver").load()
 
job.init(args['JOB_NAME'], args)

print(source_df)

##Convert DataFrames to AWS Glue's DynamicFrames Object
dynamic_dframe = DynamicFrame.fromDF(source_df, glueContext, "dynamic_df")
 
##Write Dynamic Frames to S3 in CSV format. You can write it to any rds/redshift, by using the connection that you have defined previously in Glue
datasink4 = glueContext.write_dynamic_frame.from_options(frame = dynamic_dframe, connection_type = "s3", connection_options = {"path": "s3://glueuserdata"}, format = "csv", transformation_ctx = "datasink4")
 
job.commit()
