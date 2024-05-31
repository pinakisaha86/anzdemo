import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1717173106241 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://anz-demo/curated/monthly/"], "recurse": True}, transformation_ctx="AmazonS3_node1717173106241")

# Script generated for node Amazon Redshift_rawdata
AmazonRedshift_rawdata_node1717173293765 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_node1717173106241, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-374090563854-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.macroeconomic_indicator", "connectionName": "Redshift connection anzdemo", "preactions": "CREATE TABLE IF NOT EXISTS public.macroeconomic_indicator (status VARCHAR, card_present_flag VARCHAR, account VARCHAR, currency VARCHAR, long_lat VARCHAR, txn_description VARCHAR, merchant_id VARCHAR, first_name VARCHAR, balance DOUBLE PRECISION, date VARCHAR, gender VARCHAR, age VARCHAR, merchant_suburb VARCHAR, merchant_state VARCHAR, extraction VARCHAR, amount DOUBLE PRECISION, transaction_id VARCHAR, country VARCHAR, customer_id VARCHAR, merchant_long_lat VARCHAR, movement VARCHAR, bpay_biller_code VARCHAR, merchant_code VARCHAR);"}, transformation_ctx="AmazonRedshift_rawdata_node1717173293765")

job.commit()