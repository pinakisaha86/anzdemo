from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, BooleanType,DoubleType
import os.path
import yaml

if __name__ == '__main__':
    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read Files") \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.4') \
        .getOrCreate()
        # .master('local[*]') \
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    print("\n Read Curated parquet file using 'SparkSession.read.format()'")

    df1 = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ",").parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/curated/daily/")
    df1.printSchema()
    df1.show(20, False)

    print("\n Display Cash back amount 'SparkSession.read.format()'")


    df2=df1.withColumn("Cash_Back", df1.amount*0.3)

    # Write DataFrame to CSV
    #df2.write.csv("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/publish/daily/")
    df2.write.option("header", "true").mode("overwrite").csv("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/publish/daily/")


    # spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" spark_script/daily_load.py
