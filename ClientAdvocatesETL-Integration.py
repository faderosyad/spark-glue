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
## @args: [database = "advo_integ_identity", table_name = "adv_identity_client_advocates", transformation_ctx = "client_advocates"]
## @return: client_advocates
## @inputs: []
client_advocates = glueContext.create_dynamic_frame.from_catalog(database="advo_integ_identity",
                                                                 table_name="adv_identity_client_advocates",
                                                                 transformation_ctx="client_advocates")

## @type: DataSource
## @args: [database = "advo_integ_identity", table_name = "adv_identity_user_logins", transformation_ctx = "user_logins"]
## @return: user_logins
## @inputs: []
user_logins = glueContext.create_dynamic_frame.from_catalog(database="advo_integ_identity",
                                                            table_name="adv_identity_user_logins",
                                                            transformation_ctx="<transformation_ctx>")

## @type: DataSource
## @args: [database = "advo_integ_identity", table_name = "adv_identity_advocates", transformation_ctx = "advocates"]
## @return: advocates
## @inputs: []
advocates = glueContext.create_dynamic_frame.from_catalog(database="advo_integ_identity",
                                                          table_name="adv_identity_advocates",
                                                          transformation_ctx="<transformation_ctx>")

## @type: DataSource
## @args: [database = "advo_integ_identity", table_name = "adv_identity_company", transformation_ctx = "company"]
## @return: company
## @inputs: []
company = glueContext.create_dynamic_frame.from_catalog(database="advo_integ_identity",
                                                        table_name="adv_identity_company",
                                                        transformation_ctx="<transformation_ctx>")

## @type: DataSource
## @args: [database = "advo_integ_identity", table_name = "adv_identity_roles", transformation_ctx = "roles"]
## @return: roles
## @inputs: []
roles = glueContext.create_dynamic_frame.from_catalog(database="advo_integ_identity",
                                                      table_name="adv_identity_roles",
                                                      transformation_ctx="<transformation_ctx>")

## @type: DataSource
## @args: [database = "advo_integ_identity", table_name = "adv_identity_advocate_data", transformation_ctx = "advocate_data"]
## @return: advocate_data
## @inputs: []
advocate_data = glueContext.create_dynamic_frame.from_catalog(database="advo_integ_identity",
                                                              table_name="adv_identity_advocate_data",
                                                              transformation_ctx="<transformation_ctx>")

## @type: DataSource
## @args: [database = "advo_integ_identity", table_name = "adv_identity_client_advocate_data", transformation_ctx = "client_advocate_data"]
## @return: client_advocate_data
## @inputs: []
client_advocate_data = glueContext.create_dynamic_frame.from_catalog(database="advo_integ_identity",
                                                                     table_name="adv_identity_client_advocate_data",
                                                                     transformation_ctx="<transformation_ctx>")

datapool = client_advocates.mergeDynamicFrame(roles, ["client_id"])

## @type: ApplyMapping
## @args: [mapping = [("creation_time", "timestamp", "creation_time", "timestamp"), ("role", "string", "role", "string"), ("advocate_id", "int", "advocate_id", "int"), ("sales_group_id", "int", "sales_group_id", "int"), ("xl_msisdn", "string", "xl_msisdn", "string"), ("outlet_name", "string", "outlet_name", "string"), ("created_by", "string", "created_by", "string"), ("client_id", "int", "client_id", "int"), ("parent_advocate", "int", "parent_advocate", "int"), ("client_ref_id", "string", "client_ref_id", "string"), ("zone_id", "int", "zone_id", "int"), ("last_updated_by", "string", "last_updated_by", "string"), ("last_updated_time", "timestamp", "last_updated_time", "timestamp"), ("advocate_type", "string", "advocate_type", "string"), ("work_entity", "int", "work_entity", "int"), ("status", "string", "status", "string"), ("loki_index", "long", "loki_index", "long")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = client_advocates]
applymapping1 = ApplyMapping.apply(frame=client_advocates,
                                   mappings=[("creation_time", "timestamp", "creation_time", "timestamp"),
                                             ("role", "string", "role", "string"),
                                             ("advocate_id", "int", "advocate_id", "int"),
                                             ("sales_group_id", "int", "sales_group_id", "int"),
                                             ("xl_msisdn", "string", "xl_msisdn", "string"),
                                             ("outlet_name", "string", "outlet_name", "string"),
                                             ("created_by", "string", "created_by", "string"),
                                             ("client_id", "int", "client_id", "int"),
                                             ("parent_advocate", "int", "parent_advocate", "int"),
                                             ("client_ref_id", "string", "client_ref_id", "string"),
                                             ("zone_id", "int", "zone_id", "int"),
                                             ("last_updated_by", "string", "last_updated_by", "string"),
                                             ("last_updated_time", "timestamp", "last_updated_time", "timestamp"),
                                             ("advocate_type", "string", "advocate_type", "string"),
                                             ("work_entity", "int", "work_entity", "int"),
                                             ("status", "string", "status", "string"),
                                             ("loki_index", "long", "loki_index", "long")],
                                   transformation_ctx="applymapping1")

## @type: SelectFields
## @args: [paths = ["creation_time", "role", "advocate_id", "sales_group_id", "xl_msisdn", "outlet_name", "created_by", "client_id", "parent_advocate", "client_ref_id", "zone_id", "last_updated_by", "last_updated_time", "advocate_type", "work_entity", "status", "loki_index"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(frame=applymapping1, paths=["creation_time",
                                                               "role",
                                                               "advocate_id",
                                                               "sales_group_id",
                                                               "xl_msisdn",
                                                               "outlet_name",
                                                               "created_by",
                                                               "client_id",
                                                               "parent_advocate",
                                                               "client_ref_id",
                                                               "zone_id",
                                                               "last_updated_by",
                                                               "last_updated_time",
                                                               "advocate_type",
                                                               "work_entity",
                                                               "status", "loki_index"],
                                   transformation_ctx="selectfields2")

## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "adv_integ_dw", table_name = "advotics_dw_client_advocates", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]
resolvechoice3 = ResolveChoice.apply(frame=selectfields2, choice="MATCH_CATALOG", database="adv_integ_dw",
                                     table_name="advotics_dw_client_advocates", transformation_ctx="resolvechoice3")

## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice4"]
## @return: resolvechoice4
## @inputs: [frame = resolvechoice3]
resolvechoice4 = ResolveChoice.apply(frame=resolvechoice3, choice="make_cols", transformation_ctx="resolvechoice4")

## @type: DataSink
## @args: [database = "adv_integ_dw", table_name = "advotics_dw_client_advocates", transformation_ctx = "datasink5"]
## @return: datasink5
## @inputs: [frame = resolvechoice4]
datasink5 = glueContext.write_dynamic_frame.from_catalog(frame=resolvechoice4, database="adv_integ_dw",
                                                         table_name="advotics_dw_client_advocates",
                                                         transformation_ctx="datasink5")
job.commit()
