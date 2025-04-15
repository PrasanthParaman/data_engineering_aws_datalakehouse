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

# Script generated for node AWS Glue Data Catalog - accelerometer_trusted
AWSGlueDataCatalogaccelerometer_trusted_node1744695995506 = glueContext.create_dynamic_frame.from_catalog(database="project_stedi", table_name="accelerator_trusted", transformation_ctx="AWSGlueDataCatalogaccelerometer_trusted_node1744695995506")

# Script generated for node AWS Glue Data Catalog - step_trainer_trusted
AWSGlueDataCatalogstep_trainer_trusted_node1744695997410 = glueContext.create_dynamic_frame.from_catalog(database="project_stedi", table_name="step_trainer_trusted", transformation_ctx="AWSGlueDataCatalogstep_trainer_trusted_node1744695997410")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct * 
from accelerometer_trusted
join step_trainer_trusted
on accelerometer_trusted.timeStamp = step_trainer_trusted.sensorReadingTime
'''
SQLQuery_node1744696045080 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_trusted":AWSGlueDataCatalogstep_trainer_trusted_node1744695997410, "accelerometer_trusted":AWSGlueDataCatalogaccelerometer_trusted_node1744695995506}, transformation_ctx = "SQLQuery_node1744696045080")

# Script generated for node Amazon S3 - machine_learning_curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1744696045080, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1744693773185", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3machine_learning_curated_node1744696156522 = glueContext.getSink(path="s3://data-lakes-project/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3machine_learning_curated_node1744696156522")
AmazonS3machine_learning_curated_node1744696156522.setCatalogInfo(catalogDatabase="project_stedi",catalogTableName="machine_learning_curated")
AmazonS3machine_learning_curated_node1744696156522.setFormat("json")
AmazonS3machine_learning_curated_node1744696156522.writeFrame(SQLQuery_node1744696045080)
job.commit()