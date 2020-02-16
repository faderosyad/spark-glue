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
## @args: [database = "adv-sbx-ddl", table_name = "adv_identity_client_advocates", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "adv-sbx-ddl", table_name = "adv_identity_client_advocates", transformation_ctx = "datasource0")

## @type: DataSource
## @args: [database = "adv-sbx-ddl", table_name = "adv_identity_client_roles", redshift_tmp_dir = args["TempDir"], transformation_ctx = "<transformation_ctx>"]
## @return: <output>
## @inputs: []
datasource1 = glueContext.create_dynamic_frame.from_catalog(database = "adv-sbx-ddl", table_name = "adv_identity_client_roles", transformation_ctx = "datasource1")

## @type: DataSource
## @args: [database = "adv-sbx-ddl", table_name = "adv_identity_user_logins", redshift_tmp_dir = args["TempDir"], transformation_ctx = "<transformation_ctx>"]
## @return: <output>
## @inputs: []
datasource2 = glueContext.create_dynamic_frame.from_catalog(database = "adv-sbx-ddl", table_name = "adv_identity_user_logins", transformation_ctx = "datasource2")

## @type: DataSource
## @args: [database = "adv-sbx-ddl", table_name = "adv_identity_advocates", redshift_tmp_dir = args["TempDir"], transformation_ctx = "<transformation_ctx>"]
## @return: <output>
## @inputs: []
datasource3 = glueContext.create_dynamic_frame.from_catalog(database = "adv-sbx-ddl", table_name = "adv_identity_advocates", transformation_ctx = "datasource3")

## @type: DataSource
## @args: [database = "adv-sbx-ddl", table_name = "adv_identity_advocate_data", redshift_tmp_dir = args["TempDir"], transformation_ctx = "<transformation_ctx>"]
## @return: <output>
## @inputs: []
datasource4 = glueContext.create_dynamic_frame.from_catalog(database = "adv-sbx-ddl", table_name = "adv_identity_advocate_data", transformation_ctx = "datasource4")

## @type: DataSource
## @args: [database = "adv-sbx-ddl", table_name = "adv_identity_client_advocate_data", redshift_tmp_dir = args["TempDir"], transformation_ctx = "<transformation_ctx>"]
## @return: <output>
## @inputs: []
datasource5 = glueContext.create_dynamic_frame.from_catalog(database = "adv-sbx-ddl", table_name = "adv_identity_client_advocate_data", transformation_ctx = "datasource5")

clientAdvocate = datasource0.drop_fields(['xl_msisdn', 'last_updated_time', 'last_updated_by', 'work_entity', 'loki_index', 'created_by'])\
                .rename_field('client_id','ca_client_id')\
                .rename_field('advocate_id', 'ca_advocate_id')\
                .rename_field('advocate_type', 'ca_advocate_type')\
                .rename_field('status', 'ca_status')\
                .rename_field('creation_time','ca_creation_time')
clientRoles = datasource1.drop_fields(['creation_time', 'created_by', 'last_updated_by', 'last_updated_time', 'loki_index'])\
                .rename_field('client_id','cr_client_id')\
                .rename_field('advocate_id', 'cr_advocate_id')\
                .rename_field('advocate_type','cr_advocate_type')\
                .rename_field('is_active', 'cr_is_active')
userLogins = datasource2.drop_fields(['creation_time', 'created_by', 'last_updated_by', 'last_updated_time', 'loki_index'])\
                .rename_field('')
advocates = datasource3.drop_fields(['creation_time', 'created_by', 'last_updated_by', 'last_updated_time', 'loki_index'])\
                .rename_field('advocate_id','ad_advocate_id')


datapool = Join.apply(frame1 = advocates,
                      frame2 = Join.apply(frame1 = clientRoles,
                                          frame2 = Join.apply(frame1 = userLogins,
                                                              frame2 = clientAdvocate,
                                                              keys1 = ['user_type_id'],
                                                              keys2 = ['ca.advocate_id']),
                                          keys1 = ['role_id'],
                                          keys2 = ['role']),
                      keys1 = ['user_type_id'],
                      keys2 = ['ca.advocate_id'])

## @type: ApplyMapping
## @args: [mapping = [("advocate_id", "int", "advocate_id", "int"), ("sales_group_id", "int", "sales_group_id", "int"), ("outlet_name", "string", "outlet_name", "string"), ("client_id", "int", "client_id", "int"), ("parent_advocate", "int", "parent_advocate", "int"), ("client_ref_id", "string", "client_ref_id", "string"), ("creation_time", "timestamp", "client_advocate_creation_time", "timestamp"), ("advocate_type", "string", "advocate_type", "string"), ("status", "string", "status", "string"), ("role", "string", "org_role", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datapool, mappings =
                                            [("ca_client_id", "int", "client_id", "int"),
                                            ("ad_advocate_id", "int", "advocate_id", "int"),
                                            ("ad_advocate_type", "string", "advocate_type", "string"),
                                            ("advocate_name", "string", "name", "string"),
                                            ("phone_1", "string", "phone", "string"),
                                            ("parent_advocate", "int", "parent_advocate", "int"),
                                            ("ca_status", "string", "status", "string"),
                                            ("latitude", "double", "latitude", "double"),
                                            ("longitude", "double", "longitude", "double"),
                                            ("latitude_index", "double", "latitude_index", "double"),
                                            ("longitude_index", "double", "longitude_index", "double"),
                                            ("sales_group_id", "int", "sales_group_id", "int"),
                                            ("role_id", "string", "org_role", "string"),
                                            ("role_name", "string", "org_role_name", "string"),
                                            ("user_agent", "string", "user_agent", "string"),
                                            ("client_ref_id", "string", "client_ref_id", "string"),
                                            z
                                            ("username", "string", "login_username", "string")
                                            ],
                                   transformation_ctx = "applymapping1")

## @type: SelectFields
## @args: [paths = ["creation_time", "registered_by", "registered_date_index", "latitude", "is_login_active", "advocate_id", "regency", "latitude_index", "payment_terms", "outlet_name", "client_advocate_creation_time", "client_id", "creation_month_index", "province", "advocate_data", "sales_group_name", "credit_limit", "registered_month_index", "user_agent", "longitude", "phone_2", "contact_name", "address", "assigned_distributor_id", "login_username", "sales_group_id", "store_type", "org_role", "org_role_name", "parent_advocate", "client_ref_id", "phone", "name", "postal_code", "registered_date", "advocate_type", "phone_imei", "longitude_index", "status", "creation_date_index", "loki_index"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["creation_time", "registered_by", "registered_date_index", "latitude", "is_login_active", "advocate_id", "regency", "latitude_index", "payment_terms", "outlet_name", "client_advocate_creation_time", "client_id", "creation_month_index", "province", "advocate_data", "sales_group_name", "credit_limit", "registered_month_index", "user_agent", "longitude", "phone_2", "contact_name", "address", "assigned_distributor_id", "login_username", "sales_group_id", "store_type", "org_role", "org_role_name", "parent_advocate", "client_ref_id", "phone", "name", "postal_code", "registered_date", "advocate_type", "phone_imei", "longitude_index", "status", "creation_date_index", "loki_index"], transformation_ctx = "selectfields2")

## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "adv-sbx-ddl", table_name = "advotics_dw_dw_raw_client_advocates_glue", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]
resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "adv-sbx-ddl", table_name = "advotics_dw_dw_raw_client_advocates_glue", transformation_ctx = "resolvechoice3")

## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice4"]
## @return: resolvechoice4
## @inputs: [frame = resolvechoice3]
resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")

## @type: DataSink
## @args: [database = "adv-sbx-ddl", table_name = "advotics_dw_dw_raw_client_advocates_glue", transformation_ctx = "datasink5"]
## @return: datasink5
## @inputs: [frame = resolvechoice4]
datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = "adv-sbx-ddl", table_name = "advotics_dw_dw_raw_client_advocates_glue", transformation_ctx = "datasink5")

job.commit()