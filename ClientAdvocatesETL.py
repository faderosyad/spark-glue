import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Create by: Fade Khalifah Rosyad
# Email: fade.rosyad@advotics.com
# Date: 28 January 2020
# For AWS Glue for Advotics ETL Process

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Defining Data Source

## @type: DataSource
## @args: [database = "advo-sbx-cluster", table_name = "adv_identity_client_advocates", transformation_ctx = "datasource0"]
## @return: client_advocates
## @inputs: []
client_advocates = glueContext.create_dynamic_frame.from_catalog(database = "advo-sbx-cluster",
                                                                 table_name = "adv_identity_client_advocates",
                                                                 transformation_ctx = "client_advocates")

## @type: DataSource
## @args: [database = "advo-sbx-cluster", table_name = "adv_identity_client_advocate_data", transformation_ctx = "<transformation_ctx>"]
## @return: client_advocate_data
## @inputs: []
client_advocate_data = glueContext.create_dynamic_frame.from_catalog(database = "advo-sbx-cluster",
                                                                     table_name = "adv_identity_client_advocate_data",
                                                                     transformation_ctx = "client_advocate_data")

## @type: DataSource
## @args: [database = "advo-sbx-cluster", table_name = "adv_identity_advocates", transformation_ctx = "<transformation_ctx>"]
## @return: advocates
## @inputs: []
advocates = glueContext.create_dynamic_frame.from_catalog(database = "advo-sbx-cluster",
                                                          table_name = "adv_identity_advocates",
                                                          transformation_ctx = "advocates")

## @type: DataSource
## @args: [database = "advo-sbx-cluster", table_name = "adv_identity_advocate_data", transformation_ctx = "<transformation_ctx>"]
## @return: advocates_data
## @inputs: []
advocates_data = glueContext.create_dynamic_frame.from_catalog(database = "advo-sbx-cluster",
                                                               table_name = "adv_identity_advocate_data",
                                                               transformation_ctx = "advocates_data")

## @type: DataSource
## @args: [database = "advo-sbx-cluster", table_name = "adv_identity_user_logins", transformation_ctx = "<transformation_ctx>"]
## @return: user_logins
## @inputs: []
user_logins = glueContext.create_dynamic_frame.from_catalog(database = "advo-sbx-cluster",
                                                            table_name = "adv_identity_user_logins",
                                                            transformation_ctx = "user_logins")

# Create ETL Process before Mapping to ApplyMapping

## @type: ApplyMapping
## @args: [mapping = [("creation_time", "timestamp", "creation_time", "timestamp"), ("role", "string", "phone", "string"), ("advocate_id", "int", "advocate_id", "int"), ("sales_group_id", "int", "sales_group_id", "int"), ("xl_msisdn", "string", "province", "string"), ("outlet_name", "string", "outlet_name", "string"), ("created_by", "string", "regency", "string"), ("client_id", "int", "client_id", "int"), ("parent_advocate", "int", "parent_advocate", "int"), ("client_ref_id", "string", "client_ref_id", "string"), ("zone_id", "int", "creation_date_index", "int"), ("last_updated_by", "string", "advocate_data", "string"), ("last_updated_time", "timestamp", "client_advocate_creation_time", "timestamp"), ("advocate_type", "string", "advocate_type", "string"), ("work_entity", "int", "registered_by", "int"), ("status", "string", "status", "string"), ("loki_index", "long", "loki_index", "long")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("creation_time", "timestamp", "creation_time", "timestamp"), ("role", "string", "phone", "string"), ("advocate_id", "int", "advocate_id", "int"), ("sales_group_id", "int", "sales_group_id", "int"), ("xl_msisdn", "string", "province", "string"), ("outlet_name", "string", "outlet_name", "string"), ("created_by", "string", "regency", "string"), ("client_id", "int", "client_id", "int"), ("parent_advocate", "int", "parent_advocate", "int"), ("client_ref_id", "string", "client_ref_id", "string"), ("zone_id", "int", "creation_date_index", "int"), ("last_updated_by", "string", "advocate_data", "string"), ("last_updated_time", "timestamp", "client_advocate_creation_time", "timestamp"), ("advocate_type", "string", "advocate_type", "string"), ("work_entity", "int", "registered_by", "int"), ("status", "string", "status", "string"), ("loki_index", "long", "loki_index", "long")], transformation_ctx = "applymapping1")

## @type: SelectFields
## @args: [paths = ["creation_time", "registered_by", "registered_date_index", "latitude", "is_login_active", "advocate_id", "regency", "latitude_index", "payment_terms", "outlet_name", "client_advocate_creation_time", "client_id", "creation_month_index", "province", "advocate_data", "sales_group_name", "credit_limit", "registered_month_index", "user_agent", "longitude", "phone_2", "contact_name", "address", "assigned_distributor_id", "login_username", "sales_group_id", "store_type", "org_role", "org_role_name", "parent_advocate", "client_ref_id", "phone", "name", "postal_code", "registered_date", "advocate_type", "phone_imei", "longitude_index", "status", "creation_date_index", "loki_index"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["creation_time", "registered_by", "registered_date_index", "latitude", "is_login_active", "advocate_id", "regency", "latitude_index", "payment_terms", "outlet_name", "client_advocate_creation_time", "client_id", "creation_month_index", "province", "advocate_data", "sales_group_name", "credit_limit", "registered_month_index", "user_agent", "longitude", "phone_2", "contact_name", "address", "assigned_distributor_id", "login_username", "sales_group_id", "store_type", "org_role", "org_role_name", "parent_advocate", "client_ref_id", "phone", "name", "postal_code", "registered_date", "advocate_type", "phone_imei", "longitude_index", "status", "creation_date_index", "loki_index"], transformation_ctx = "selectfields2")

## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "advo-sbx-dw", table_name = "advotics_dw_dw_raw_client_advocates", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]
resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "advo-sbx-dw", table_name = "advotics_dw_dw_raw_client_advocates", transformation_ctx = "resolvechoice3")

## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice4"]
## @return: resolvechoice4
## @inputs: [frame = resolvechoice3]
resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")

## @type: DataSink
## @args: [database = "advo-sbx-dw", table_name = "advotics_dw_dw_raw_client_advocates", transformation_ctx = "datasink5"]
## @return: datasink5
## @inputs: [frame = resolvechoice4]
datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = "advo-sbx-dw", table_name = "advotics_dw_dw_raw_client_advocates", transformation_ctx = "datasink5")
job.commit()