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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1744689746816 = glueContext.create_dynamic_frame.from_catalog(database="project_stedi", table_name="customer_landing", transformation_ctx="AWSGlueDataCatalog_node1744689746816")

# Script generated for node SQL Query - Filter
SqlQuery0 = '''
select * from myDataSource
where sharewithresearchasofdate is not null;
'''
SQLQueryFilter_node1744689877947 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":AWSGlueDataCatalog_node1744689746816}, transformation_ctx = "SQLQueryFilter_node1744689877947")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQueryFilter_node1744689877947, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1744689716749", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1744690511571 = glueContext.getSink(path="s3://data-lakes-project/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1744690511571")
AmazonS3_node1744690511571.setCatalogInfo(catalogDatabase="project_stedi",catalogTableName="customer_trusted")
AmazonS3_node1744690511571.setFormat("json")
AmazonS3_node1744690511571.writeFrame(SQLQueryFilter_node1744689877947)
job.commit()