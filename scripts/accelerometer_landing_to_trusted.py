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

# Script generated for node AWS Glue Data Catalog - accelerator_landing
AWSGlueDataCatalogaccelerator_landing_node1744690985905 = glueContext.create_dynamic_frame.from_catalog(database="project_stedi", table_name="accelerometer_landing", transformation_ctx="AWSGlueDataCatalogaccelerator_landing_node1744690985905")

# Script generated for node AWS Glue Data Catalog - customer_trusted
AWSGlueDataCatalogcustomer_trusted_node1744690984827 = glueContext.create_dynamic_frame.from_catalog(database="project_stedi", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalogcustomer_trusted_node1744690984827")

# Script generated for node SQL Query - filter only consented
SqlQuery0 = '''
select accelerator_landing.*
from customer_trusted join accelerator_landing 
on customer_trusted.email = accelerator_landing.user

'''
SQLQueryfilteronlyconsented_node1744691202759 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_trusted":AWSGlueDataCatalogcustomer_trusted_node1744690984827, "accelerator_landing":AWSGlueDataCatalogaccelerator_landing_node1744690985905}, transformation_ctx = "SQLQueryfilteronlyconsented_node1744691202759")

# Script generated for node Amazon S3 - accelerator_trusted
EvaluateDataQuality().process_rows(frame=SQLQueryfilteronlyconsented_node1744691202759, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1744689716749", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3accelerator_trusted_node1744691348379 = glueContext.getSink(path="s3://data-lakes-project/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3accelerator_trusted_node1744691348379")
AmazonS3accelerator_trusted_node1744691348379.setCatalogInfo(catalogDatabase="project_stedi",catalogTableName="accelerator_trusted")
AmazonS3accelerator_trusted_node1744691348379.setFormat("json")
AmazonS3accelerator_trusted_node1744691348379.writeFrame(SQLQueryfilteronlyconsented_node1744691202759)
job.commit()