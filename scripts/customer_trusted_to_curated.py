import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog - accelerameter_trusted
AWSGlueDataCatalogaccelerameter_trusted_node1744692447168 = glueContext.create_dynamic_frame.from_catalog(database="project_stedi", table_name="accelerator_trusted", transformation_ctx="AWSGlueDataCatalogaccelerameter_trusted_node1744692447168")

# Script generated for node AWS Glue Data Catalog - customer_trusted
AWSGlueDataCatalogcustomer_trusted_node1744692407985 = glueContext.create_dynamic_frame.from_catalog(database="project_stedi", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalogcustomer_trusted_node1744692407985")

# Script generated for node SQL Query - join customer and accelerometer trusted
SqlQuery8800 = '''
select distinct *
from customer_trusted
where email in (select distinct user from accelerometer_trusted)


'''
SQLQueryjoincustomerandaccelerometertrusted_node1744692479517 = sparkSqlQuery(glueContext, query = SqlQuery8800, mapping = {"accelerometer_trusted":AWSGlueDataCatalogaccelerameter_trusted_node1744692447168, "customer_trusted":AWSGlueDataCatalogcustomer_trusted_node1744692407985}, transformation_ctx = "SQLQueryjoincustomerandaccelerometertrusted_node1744692479517")

# Script generated for node Amazon S3 - customer_curated
EvaluateDataQuality().process_rows(frame=SQLQueryjoincustomerandaccelerometertrusted_node1744692479517, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1744689716749", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3customer_curated_node1744692576254 = glueContext.getSink(path="s3://data-lakes-project/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3customer_curated_node1744692576254")
AmazonS3customer_curated_node1744692576254.setCatalogInfo(catalogDatabase="project_stedi",catalogTableName="customer_curated")
AmazonS3customer_curated_node1744692576254.setFormat("json")
AmazonS3customer_curated_node1744692576254.writeFrame(SQLQueryjoincustomerandaccelerometertrusted_node1744692479517)
job.commit()