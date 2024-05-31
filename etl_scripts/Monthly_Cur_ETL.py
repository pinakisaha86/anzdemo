import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount >= max(last(1)),
        RowCount > avg(last(10))*0.6
    ]
"""

# Script generated for node Amazon S3_landing
AmazonS3_landing_node1717176482175 = glueContext.create_dynamic_frame.from_catalog(database="anz_montly_transac", table_name="monthly", transformation_ctx="AmazonS3_landing_node1717176482175")

# Script generated for node curated S3
EvaluateDataQuality().process_rows(frame=AmazonS3_landing_node1717176482175, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1717174596511", "enableDataQualityResultsPublishing": True}, additional_options={})
curatedS3_node1717176622648 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_landing_node1717176482175, connection_type="s3", format="glueparquet", connection_options={"path": "s3://anz-demo/curated/monthly/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="curatedS3_node1717176622648")

job.commit()