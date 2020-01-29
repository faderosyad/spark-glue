import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import *
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

db_name = "glue_testing_oltp"

# OLTP Client Advocate
client_advocates = glueContext.create_dynamic_frame.from_catalog\
    (database = db_name, table_name = "testing_oltp_client_advocates", transformation_ctx = "client_advocates")

# OLTP user logins
user_logins = glueContext.create_dynamic_frame.from_catalog\
    (database = db_name, table_name = "testing_oltp_user_login", transformation_ctx = "user_logins")

# Joining the frames
joined_table = client_advocates.join(user_logins, client_advocates.advocate_id == user_logins.user_type_id)

# Joining frames
table_joined = Join.apply(client_advocates, user_logins, 'advocate_id', 'user_type_id')

#client advocates to dw raw client advocate
applymapping1 = ApplyMapping.apply(frame = joined_table, mappings = [("name", "string", "name", "string"),
                                                                         ("advocate_type", "string", "advocate_type", "string"),
                                                                         ("creation_time", "timestamp", "creation_time", "timestamp"),
                                                                         ("phone_1", "string", "phone", "string"),
                                                                         ("last_updated_time", "timestamp", "last_updated_time", "timestamp"),
                                                                         ("advocate_id", "int", "advocate_id", "int"),
                                                                         ("client_id", "int", "client_id", "int"),
                                                                         ("status", "string", "status", "string"),
                                                                         ("sales_group_id", "int","sales_group_id", "int"),
                                                                         ("sales_group_name", "string", "sales_group_name", "string"),
                                                                         ("org_role", "string", "org_role", "string"),
                                                                         ("org_role_name", "string", "org_role_name", "string"),
                                                                         ("client_ref_id", "string", "client_ref_id", "string"),
                                                                         ("loki_index", "long", "loki_index", "long")],
                                   transformation_ctx = "applymapping1")

selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["creation_time", "advocate_id", "login_username",
                                                                   "sales_group_id", "org_role", "org_role_name",
                                                                   "client_advocate_creation_time", "client_id",
                                                                   "parent_advocate", "client_ref_id", "phone", "name",
                                                                   "sales_group_name", "advocate_type", "status",
                                                                   "loki_index"], transformation_ctx = "selectfields2")

resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "glue_testing_olap", table_name = "testing_olap_dw_raw_client_advocates", transformation_ctx = "resolvechoice3")

resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")

datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = "glue_testing_olap", table_name = "testing_olap_dw_raw_client_advocates", transformation_ctx = "datasink5")
job.commit()