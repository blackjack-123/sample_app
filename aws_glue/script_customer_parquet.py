import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="glue_db", table_name="customers_csv", transformation_ctx="S3bucket_node1"
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("customerid", "long", "customerid", "long"),
        ("namestyle", "boolean", "namestyle", "boolean"),
        ("title", "string", "title", "string"),
        ("firstname", "string", "firstname", "string"),
        ("middlename", "string", "middlename", "string"),
        ("lastname", "string", "lastname", "string"),
        ("suffix", "string", "suffix", "string"),
        ("companyname", "string", "companyname", "string"),
        ("salesperson", "string", "salesperson", "string"),
        ("emailaddress", "string", "emailaddress", "string"),
        ("phone", "string", "phone", "string"),
        ("passwordhash", "string", "passwordhash", "string"),
        ("passwordsalt", "string", "passwordsalt", "string"),
        ("rowguid", "string", "rowguid", "string"),
        ("modifieddate", "string", "modifieddate", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node2,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://anish-blackjack-6674-glue/data/customers_database/customers_parquet/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
