import sys
import os
import time
import datetime
import pytz
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark.sql import *
from pyspark.sql.functions import lit
from datetime import datetime, timedelta
from pytz import timezone

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','loki_start','loki_end','client_identifier'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

utc = pytz.utc
jakarta = pytz.timezone('Asia/Jakarta')

# Term
LOKI_START = args['loki_start']
LOKI_END = args['loki_end']

clientIdentifier = args['client_identifier']

PHONE_2_DATA_KEY = "PHONE_NUMBER_1"
IMEI_1_DATA_KEY = "IMEI_1"
ASSIGNED_DISTRIBUTOR_DATA_KEY = "ASSIGNED_FULFILLER_ID"
DISTRIBUTOR_CODE_DATA_KEY = "DISTRIBUTOR_CODE"
PAYMENT_TERMS_DATA_KEY = "PAYMENT_TERMS"
CREDIT_LIMIT_DATA_KEY = "CREDIT_LIMIT"
CONTACT_NAME_DATA_KEY = "CONTACT_NAME"
STORE_TYPE_DATA_KEY = "STORE_TYPE"
REFERRER_ID_KEY = "REFERRER_ID"
ACTIVATOR_ID_KEY = "ACTIVATOR_ID"
ACTIVATION_TIME_KEY = "ACTIVATION_TIME"
DEFAULT_ADDRESS_SEQ = 1
ADDRESS_1 = "ADDRESS_1"
ADVOCATE_DATA_MAP_KEY = "advocateData"
CLIENT_ADVOCATE_DATA_MAP_KEY = "clientAdvocateData"
DEFAULT_REGENCY = "3171"
DEFAULT_PROVINCE = "31"

clientAdvocates = glueContext.create_dynamic_frame.from_catalog(database="advo-sbx-cluster",
                                                                table_name="adv_identity_client_advocates",
                                                                transformation_ctx="clientAdvocates").toDF()

clientRoles = glueContext.create_dynamic_frame.from_catalog(database="advo-sbx-cluster",
                                                            table_name="adv_identity_client_roles",
                                                            transformation_ctx="clientRoles").toDF()

userLogins = glueContext.create_dynamic_frame.from_catalog(database="advo-sbx-cluster",
                                                           table_name="adv_identity_user_logins",
                                                           transformation_ctx="userLogins").toDF()

advocates = glueContext.create_dynamic_frame.from_catalog(database="advo-sbx-cluster",
                                                          table_name="adv_identity_advocates",
                                                          transformation_ctx="advocates").toDF()

advocateAddress = glueContext.create_dynamic_frame.from_catalog(database="advo-sbx-cluster",
                                                                table_name="adv_identity_advocate_address",
                                                                transformation_ctx="advocateAddress").toDF()

company = glueContext.create_dynamic_frame.from_catalog(database="advo-sbx-cluster",
                                                        table_name="adv_identity_company",
                                                        transformation_ctx="company").toDF()

advocateData = glueContext.create_dynamic_frame.from_catalog(database="advo-sbx-cluster",
                                                             table_name="adv_identity_advocate_data",
                                                             transformation_ctx="advocateData").toDF()

clientAdvocateData = glueContext.create_dynamic_frame.from_catalog(database="advo-sbx-cluster",
                                                                  table_name="adv_identity_client_advocate_data",
                                                                  transformation_ctx="clientAdvocateData").toDF()

# Data Cleansing
clientid1 = clientAdvocates \
    .filter((clientAdvocates.client_id == clientIdentifier) & (col("loki_index").between(LOKI_START, LOKI_END))) \
    .drop('xl_msisdn', 'last_updated_time', 'last_updated_by', 'work_entity', 'loki_index', 'created_by')

clientroles1 = clientRoles \
    .filter(clientRoles.client_id == clientIdentifier) \
    .drop('creation_time', 'created_by', 'last_updated_by', 'last_updated_time', 'loki_index', 'advocate_type')

userlogins1 = userLogins \
    .drop('creation_time', 'created_by', 'last_updated_by', 'last_updated_time', 'loki_index') \
    .withColumnRenamed('is_active', 'is_login_active')

advocates1 = advocates.drop('creation_time', 'created_by', 'last_updated_by', 'last_updated_time', 'loki_index',
                            'latitude', 'longitude', 'latitude_index', 'longitude_index', 'postal_code')

company1 = company\
    .filter(company.client_id == clientIdentifier) \
    .drop('creation_time', 'created_by', 'last_updated_by', 'last_updated_time', 'loki_index')

advocateAddress1 = advocateAddress \
    .filter(advocateAddress.address_seq == "1") \
    .drop('creation_time', 'created_by', 'last_updated_by', 'last_updated_time', 'loki_index', 'is_active')

advocateData1 = advocateData \
    .drop('creation_time', 'created_by', 'last_updated_by', 'last_updated_time', 'loki_index')

clientAdvocateData1 = clientAdvocateData \
    .filter(clientAdvocateData.client_id == clientIdentifier) \
    .withColumnRenamed('client_id', 'clientid_advocatedata') \
    .drop('creation_time', 'created_by', 'last_updated_by', 'last_updated_time', 'loki_index')

assignedDistributor = advocateData1 \
    .filter(advocateData1.data_key == ASSIGNED_DISTRIBUTOR_DATA_KEY) \
    .withColumnRenamed('advocate_id', 'advocate_id_dist')

distributorCode = clientAdvocateData1 \
    .filter(clientAdvocateData1.data_key == DISTRIBUTOR_CODE_DATA_KEY) \
    .withColumnRenamed('data_value', 'assigned_distributor_id') \
    .withColumnRenamed('advocate_id', 'advocate_id_1')

paymentTerms = clientAdvocateData1 \
    .filter(clientAdvocateData1.data_key == PAYMENT_TERMS_DATA_KEY) \
    .withColumnRenamed('data_value', 'payment_terms') \
    .withColumnRenamed('advocate_id', 'advocate_id_2')

creditLimits = clientAdvocateData1 \
    .filter(clientAdvocateData1.data_key == CREDIT_LIMIT_DATA_KEY) \
    .withColumnRenamed('data_value', 'credit_limit') \
    .withColumnRenamed('advocate_id', 'advocate_id_3')

storeType = clientAdvocateData1 \
    .filter(clientAdvocateData1.data_key == STORE_TYPE_DATA_KEY) \
    .withColumnRenamed('data_value', 'store_type') \
    .withColumnRenamed('advocate_id', 'advocate_id_4')

# Joining all the table
datajoin = clientid1.join(advocates1, clientid1.advocate_id == advocates1.advocate_id, how='left_outer') \
    .drop(advocates1.advocate_id)

datajoin2 = datajoin.join(userlogins1, datajoin.advocate_id == userlogins1.user_type_id, how='left_outer') \
    .drop(userlogins1.user_type_id) \
    .fillna({'username': "NON", 'is_login_active': 'N'})

datajoin3 = datajoin2.join(clientroles1, datajoin2.role == clientroles1.role_code, how='left_outer') \
    .drop(clientroles1.role_code) \
    .drop(clientroles1.client_id)

datajoin4 = datajoin3.join(company1, datajoin3.sales_group_id == company1.sales_group_id, how='left_outer') \
    .drop(company1.sales_group_id) \
    .drop(company1.client_id)

datajoin5 = datajoin4.join(advocateAddress1, datajoin4.advocate_id == advocateAddress1.advocate_id, how='left_outer') \
    .drop(advocateAddress1.advocate_id) \
    .fillna({'regency': DEFAULT_REGENCY, 'province': DEFAULT_PROVINCE}) \
    .withColumn("lokiindex", lit(datetime.now() + timedelta(hours=7)))

# datajoin6 = datajoin5.join(assignedDistributor, datajoin5.advocate_id == assignedDistributor.advocate_id_dist, how='left_outer')\
#     .drop(assignedDistributor.advocate_id_dist)

datajoin7 = datajoin5.join(paymentTerms, datajoin5.advocate_id == paymentTerms.advocate_id_2, how='left_outer')\
    .drop(paymentTerms.advocate_id_2)

datajoin8 = datajoin7.join(creditLimits, datajoin7.advocate_id == creditLimits.advocate_id_3, how='left_outer')\
    .drop(creditLimits.advocate_id_3)

datajoin9 = datajoin8.join(storeType, datajoin8.advocate_id == storeType.advocate_id_4, how='left_outer')\
    .drop(storeType.advocate_id_4)

# Change Dataframe to DynamicFrame
dynamicPool2 = DynamicFrame.fromDF(datajoin5, glueContext, "datapool3")

applymapping1 = ApplyMapping.apply(frame=dynamicPool2, mappings=[("client_id", "int", "client_id", "int"),
                                                                 ("advocate_id", "int", "advocate_id", "int"),
                                                                 ("advocate_type", "string", "advocate_type", "string"),
                                                                 ("advocate_name", "string", "name", "string"),
                                                                 ("phone_1", "string", "phone", "string"),
                                                                 ("parent_advocate", "int", "parent_advocate", "int"),
                                                                 ("status", "string", "status", "string"),
                                                                 ("latitude", "double", "latitude", "double"),
                                                                 ("longitude", "double", "longitude", "double"),
                                                                 ("latitude_index", "double", "latitude_index", "double"),
                                                                 ("longitude_index", "double", "longitude_index", "double"),
                                                                 ("role", "string", "org_role", "string"),
                                                                 ("role_name", "string", "org_role_name", "string"),
                                                                 ("user_agent", "string", "user_agent", "string"),
                                                                 ("sales_group_id", "int", "sales_group_id", "int"),
                                                                 ("company_name", "string", "sales_group_name", "string"),
                                                                 ("client_ref_id", "string", "client_ref_id", "string"),
                                                                 ("username", "string", "login_username", "string"),
                                                                 ("outlet_name", "string", "outlet_name", "string"),
                                                                 ("is_login_active", "string", "is_login_active", "string"),
                                                                 ("address_1", "string", "address", "string"),
                                                                 ("regency", "string", "regency", "string"),
                                                                 ("province", "string", "province", "string"),
                                                                 ("postal_code", "int", "postal_code", "int"),
                                                                 ("lokiindex", "int", "loki_index", "int"),
                                                                 ("creation_time", "timestamp", "client_advocate_creation_time", "timestamp"),
                                                                 ("assigned_distributor_id", "string", "assigned_distributor_id", "string"),
                                                                 ("payment_terms", "string", "payment_terms", "string"),
                                                                 ("credit_limit", "double", "credit_limit", "double"),
                                                                 ("store_type", "string", "store_type", "string")
                                                                 ], transformation_ctx="applymapping1")

selectfields2 = SelectFields.apply(frame=applymapping1, paths=["creation_time",
                                                               "registered_by",
                                                               "registered_date_index",
                                                               "latitude",
                                                               "is_login_active",
                                                               "advocate_id",
                                                               "regency",
                                                               "latitude_index",
                                                               "payment_terms",
                                                               "outlet_name",
                                                               "client_advocate_creation_time",
                                                               "client_id",
                                                               "creation_month_index",
                                                               "province",
                                                               "advocate_data",
                                                               "sales_group_name",
                                                               "credit_limit",
                                                               "registered_month_index",
                                                               "user_agent",
                                                               "longitude",
                                                               "phone_2",
                                                               "contact_name",
                                                               "address",
                                                               "assigned_distributor_id",
                                                               "login_username",
                                                               "sales_group_id",
                                                               "store_type",
                                                               "org_role",
                                                               "org_role_name",
                                                               "parent_advocate",
                                                               "client_ref_id",
                                                               "phone",
                                                               "name",
                                                               "postal_code",
                                                               "registered_date",
                                                               "advocate_type",
                                                               "phone_imei",
                                                               "longitude_index",
                                                               "status",
                                                               "creation_date_index",
                                                               "loki_index"
                                                               ], transformation_ctx="selectfields2")

resolvechoice3 = ResolveChoice.apply(frame=selectfields2,
                                     choice="MATCH_CATALOG",
                                     database="adv-sbx-ddl",
                                     table_name="advotics_dw_dw_raw_client_advocates_glue24",
                                     transformation_ctx="resolvechoice3")

resolvechoice4 = ResolveChoice.apply(frame=resolvechoice3,
                                     choice="make_cols",
                                     transformation_ctx="resolvechoice4")

datasink5 = glueContext.write_dynamic_frame.from_catalog(frame=resolvechoice4, database="adv-sbx-ddl",
                                                         table_name="advotics_dw_dw_raw_client_advocates_glue24",
                                                         transformation_ctx="datasink5")

job.commit()