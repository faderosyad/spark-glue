import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import *
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## @type: DataSource
## @args: [database = "advo-sbx-cluster", table_name = "adv_identity_client_advocates", transformation_ctx = "client_advocates"]
## @return: client_advocates
## @inputs: []
client_advocates = glueContext.create_dynamic_frame.from_catalog(database="advo-sbx-cluster",
                                                                 table_name="adv_identity_client_advocates")

## @type: DataSource
## @args: [database = "advo-sbx-cluster", table_name = "adv_identity_user_logins", transformation_ctx = "user_logins"]
## @return: user_logins
## @inputs: []
user_logins = glueContext.create_dynamic_frame.from_catalog(database="advo-sbx-cluster",
                                                            table_name="adv_identity_user_logins")

## @type: DataSource
## @args: [database = "advo-sbx-cluster", table_name = "adv_identity_advocates", transformation_ctx = "advocates"]
## @return: advocates
## @inputs: []
advocates = glueContext.create_dynamic_frame.from_catalog(database="advo-sbx-cluster",
                                                          table_name="adv_identity_advocates")

## @type: DataSource
## @args: [database = "advo-sbx-cluster", table_name = "adv_identity_company", transformation_ctx = "company"]
## @return: company
## @inputs: []
company = glueContext.create_dynamic_frame.from_catalog(database="advo-sbx-cluster",
                                                        table_name="adv_identity_company")

## @type: DataSource
## @args: [database = "advo-sbx-cluster", table_name = "adv_identity_roles", transformation_ctx = "roles"]
## @return: roles
## @inputs: []
client_roles = glueContext.create_dynamic_frame.from_catalog(database="advo-sbx-cluster",
                                                             table_name="adv_identity_roles")

## @type: DataSource
## @args: [database = "advo-sbx-cluster", table_name = "adv_identity_advocate_data", transformation_ctx = "advocate_data"]
## @return: advocate_data
## @inputs: []
advocate_data = glueContext.create_dynamic_frame.from_catalog(database="advo-sbx-cluster",
                                                              table_name="adv_identity_advocate_data")

## @type: DataSource
## @args: [database = "advo-sbx-cluster", table_name = "adv_identity_client_advocate_data", transformation_ctx = "client_advocate_data"]
## @return: client_advocate_data
## @inputs: []
client_advocate_data = glueContext.create_dynamic_frame.from_catalog(database="advo-sbx-cluster",
                                                                     table_name="adv_identity_client_advocate_data")

## @type: DropFields
## @args: [paths = [<paths>], transformation_ctx = "<transformation_ctx>"]
## @return: <output>
## @inputs: [frame = <frame>]
# client_roles_pool = DropFields.apply(frame = client_roles, paths = ["role_description", "advovate_type", "is_active", "role_level", "display_order", "is_has_distributor", "loki_index", "last_updated_time", "last_updated_by", "creation_time", "created_by"])
## @type: Join
## @args: [keys1 = [<keys1>], keys2 = [<keys2>]]
## @return: <output>
## @inputs: [frame1 = <frame1>, frame2 = <frame2>]
datapool = Join.apply(frame1=client_advocates, frame2=client_roles, keys1=["role", "client_id"],
                      keys2=["org_role", "client_id"])

## @type: DropFields
## @args: [paths = [<paths>], transformation_ctx = "<transformation_ctx>"]
## @return: <output>
## @inputs: [frame = <frame>]
# advocates_pool = DropFields.apply(frame = advocates, paths = ["geometry_point", "loki_index", "last_updated_time", "last_updated_by", "creation_time", "creation_by"])
## @type: Join
## @args: [keys1 = [<keys1>], keys2 = [<keys2>]]
## @return: <output>
## @inputs: [frame1 = <frame1>, frame2 = <frame2>]
datapool = Join.apply(frame1=datapool, frame2=advocates, keys1=["advocate_id"], keys2=["advocate_id"])

## @type: DropFields
## @args: [paths = [<paths>], transformation_ctx = "<transformation_ctx>"]
## @return: <output>
## @inputs: [frame = <frame>]
# user_logins_pool = DropFields.apply(frame = user_logins, paths = ["role_description", "advocate_type", "is_active", "role_level", "display_order", "it_has_distributor", "loki_index", "last_updated_time", "last_updated_by", "creation_time", "created_by"])
## @type: Join
## @args: [keys1 = [<keys1>], keys2 = [<keys2>]]
## @return: <output>
## @inputs: [frame1 = <frame1>, frame2 = <frame2>]
datapool = Join.apply(frame1=datapool, frame2=user_logins, keys1=["advocate_id"], keys2=["user_type_id"])

# datapool_df = datapool.toDF()
# datapool.show()
# datapool_dynamic = DynamicFrame.fromDF(datapool_df, glueContext, "datapool_dynamic")
datapool = Join.apply(frame1 = advocates,
                      frame2 = Join.apply(frame1 = clientAdvocate, frame2 = clientRoles, keys1 = ['ca_client_id' and 'role'], keys2 = ['cr.client_id' & 'org_role']),
                      keys1 = ['user_type_id'],
                      keys2 = ['ca.advocate_id'])

applymapping1 = ApplyMapping.apply(frame = datapool, mappings =
                                            [("ad_advocate_id", "int", "advocate_id", "int"),
                                             ("sales_group_id", "int", "sales_group_id", "int"),
                                             ("outlet_name", "string", "outlet_name", "string"),
                                             ("ca_client_id", "int", "client_id", "int"),
                                             ("parent_advocate", "int", "parent_advocate", "int"),
                                             ("client_ref_id", "string", "client_ref_id", "string"),
                                             ("ca.creation_time", "timestamp", "client_advocate_creation_time", "timestamp"),
                                             ("ad_advocate_type", "string", "advocate_type", "string"),
                                             ("ca_status", "string", "status", "string"),
                                             ("role", "string", "org_role", "string"),
                                             ("role_name", "string", "org_role_name", "string"),
                                             ("user_agent", "string", "user_agent", "string"),
                                             ("phone_1", "string", "phone", "string"),
                                             ("advocate_name", "string", "name", "string"),
                                             ("latitude", "double", "latitude", "double"),
                                             ("longitude", "double", "longitude", "double"),
                                             ("latitude_index", "double", "latitude_index", "double"),
                                             ("longitude_index", "double", "longitude_index", "double")],
                                   transformation_ctx = "applymapping1")

# @type: ApplyMapping
# @args: [mapping = [("creation_time", "timestamp", "creation_time", "timestamp"), ("role", "string", "role", "string"), ("advocate_id", "int", "advocate_id", "int"), ("sales_group_id", "int", "sales_group_id", "int"), ("xl_msisdn", "string", "xl_msisdn", "string"), ("outlet_name", "string", "outlet_name", "string"), ("created_by", "string", "created_by", "string"), ("client_id", "int", "client_id", "int"), ("parent_advocate", "int", "parent_advocate", "int"), ("client_ref_id", "string", "client_ref_id", "string"), ("zone_id", "int", "zone_id", "int"), ("last_updated_by", "string", "last_updated_by", "string"), ("last_updated_time", "timestamp", "last_updated_time", "timestamp"), ("advocate_type", "string", "advocate_type", "string"), ("work_entity", "int", "work_entity", "int"), ("status", "string", "status", "string"), ("loki_index", "long", "loki_index", "long")], transformation_ctx = "applymapping1"]
# @return: applymapping1
# @inputs: [frame = client_advocates]
applymapping1 = ApplyMapping.apply(frame=datapool,
                                   mappings=[("creation_time", "timestamp", "creation_time", "timestamp"),
                                             ("role", "string", "org_role", "string"),
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
                                             ("loki_index", "long", "loki_index", "long"),
                                             ("role_name", "string", "org_role_name", "string"),
                                             ("user_agent", "string", "user_agent", "string"),
                                             ("phone_1", "string", "phone", "string"),
                                             ("advocate_name", "string", "name", "string"),
                                             ("latitude", "double", "latitude", "double"),
                                             ("longitude", "double", "longitude", "double"),
                                             ("latitude_index", "double", "latitude_index", "double"),
                                             ("longitude_index", "double", "longitude_index", "double")
                                             ],
                                   transformation_ctx="applymapping1")

# @type: SelectFields
# @args: [paths = ["creation_time", "role", "advocate_id", "sales_group_id", "xl_msisdn", "outlet_name", "created_by", "client_id", "parent_advocate", "client_ref_id", "zone_id", "last_updated_by", "last_updated_time", "advocate_type", "work_entity", "status", "loki_index"], transformation_ctx = "selectfields2"]
# @return: selectfields2
# @inputs: [frame = applymapping1]
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
                                                               "status",
                                                               "loki_index",
                                                               "org_role_name",
                                                               "user_agent",
                                                               "phone",
                                                               "name",
                                                               "latitude",
                                                               "longitude",
                                                               "latitude_index",
                                                               "longitude_index"
                                                               ],
                                   transformation_ctx="selectfields2")

## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "adv_integ_dw", table_name = "advotics_dw_dw_raw_client_advocates_glue", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]
resolvechoice3 = ResolveChoice.apply(frame=selectfields2, choice="MATCH_CATALOG", database="adv_integ_dw",
                                     table_name="advotics_dw_dw_raw_client_advocates_glue",
                                     transformation_ctx="resolvechoice3")
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice4"]
## @return: resolvechoice4
## @inputs: [frame = resolvechoice3]
resolvechoice4 = ResolveChoice.apply(frame=resolvechoice3, choice="make_cols", transformation_ctx="resolvechoice4")
## @type: DataSink
## @args: [database = "adv_integ_dw", table_name = "advotics_dw_dw_raw_client_advocates_glue", transformation_ctx = "datasink5"]
## @return: datasink5
## @inputs: [frame = resolvechoice4]
datasink5 = glueContext.write_dynamic_frame.from_catalog(frame=resolvechoice4, database="adv_integ_dw",
                                                         table_name="advotics_dw_dw_raw_client_advocates_glue",
                                                         transformation_ctx="datasink5")

job.commit()