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

# Script generated for node AWS Glue Data Catalog - step_trainer_landing
AWSGlueDataCatalogstep_trainer_landing_node1744694676449 = glueContext.create_dynamic_frame.from_catalog(database="project_stedi", table_name="step_trainer_landing", transformation_ctx="AWSGlueDataCatalogstep_trainer_landing_node1744694676449")

# Script generated for node AWS Glue Data Catalog - customer_curated
AWSGlueDataCatalogcustomer_curated_node1744694775172 = glueContext.create_dynamic_frame.from_catalog(database="project_stedi", table_name="customer_curated", transformation_ctx="AWSGlueDataCatalogcustomer_curated_node1744694775172")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct * 
from step_trainer_landing
where serialNumber in (select distinct serialNumber
from customer_curated)
'''
SQLQuery_node1744694851759 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_curated":AWSGlueDataCatalogcustomer_curated_node1744694775172, "step_trainer_landing":AWSGlueDataCatalogstep_trainer_landing_node1744694676449}, transformation_ctx = "SQLQuery_node1744694851759")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1744694851759, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1744693773185", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1744694964586 = glueContext.getSink(path="s3://data-lakes-project/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1744694964586")
AmazonS3_node1744694964586.setCatalogInfo(catalogDatabase="project_stedi",catalogTableName="step_trainer_trusted")
AmazonS3_node1744694964586.setFormat("json")
AmazonS3_node1744694964586.writeFrame(SQLQuery_node1744694851759)
job.commit()