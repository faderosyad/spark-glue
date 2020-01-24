import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "learningdb-testing", table_name = "learningdb_oltp", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "learningdb-testing", table_name = "learningdb_oltp", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("client_ref_id", "string", "client_ref_id", "string"), ("phone", "string", "phone", "string"), ("name", "string", "name", "string"), ("is_login_active", "string", "is_login_active", "string"), ("sales_group_name", "string", "sales_group_name", "string"), ("advocate_id", "int", "advocate_id", "int"), ("sales_group_id", "int", "sales_group_id", "int"), ("advocate_type", "string", "advocate_type", "string"), ("org_role", "string", "org_role", "string"), ("org_role_name", "string", "org_role_name", "string"), ("client_id", "int", "client_id", "int"), ("status", "string", "status", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("client_ref_id", "string", "client_ref_id", "string"), ("phone", "string", "phone", "string"), ("name", "string", "name", "string"), ("is_login_active", "string", "is_login_active", "string"), ("sales_group_name", "string", "sales_group_name", "string"), ("advocate_id", "int", "advocate_id", "int"), ("sales_group_id", "int", "sales_group_id", "int"), ("advocate_type", "string", "advocate_type", "string"), ("org_role", "string", "org_role", "string"), ("org_role_name", "string", "org_role_name", "string"), ("client_id", "int", "client_id", "int"), ("status", "string", "status", "string")], transformation_ctx = "applymapping1")
## @type: SelectFields
## @args: [paths = ["client_ref_id", "phone", "name", "is_login_active", "sales_group_name", "advocate_id", "sales_group_id", "advocate_type", "org_role", "org_role_name", "client_id", "status"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["client_ref_id", "phone", "name", "is_login_active", "sales_group_name", "advocate_id", "sales_group_id", "advocate_type", "org_role", "org_role_name", "client_id", "status"], transformation_ctx = "selectfields2")
## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "learningdb-testing-olap", table_name = "learningdb_olap", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]
resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "learningdb-testing-olap", table_name = "learningdb_olap", transformation_ctx = "resolvechoice3")
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice4"]
## @return: resolvechoice4
## @inputs: [frame = resolvechoice3]
resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")
## @type: DataSink
## @args: [database = "learningdb-testing-olap", table_name = "learningdb_olap", transformation_ctx = "datasink5"]
## @return: datasink5
## @inputs: [frame = resolvechoice4]
datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = "learningdb-testing-olap", table_name = "learningdb_olap", transformation_ctx = "datasink5")
job.commit()
