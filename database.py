import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.shell import sqlContext
from pyspark.sql import *
from awsglue.context import GlueContext
from awsglue.job import Job

# Defining Data Source

## @type: DataSource
## @args: [database = "advo-sbx-cluster", table_name = "adv_identity_client_advocates", transformation_ctx = "client_advocates"]
## @return: client_advocates
## @inputs: []
client_advocates = glueContext.create_dynamic_frame.from_catalog(database = "advo-sbx-cluster",
                                                                 table_name = "adv_identity_client_advocates",
                                                                 transformation_ctx = "client_advocates")

## @type: DataSource
## @args: [database = "advo-sbx-cluster", table_name = "adv_identity_client_advocate_data", transformation_ctx = "client_advocate_data"]
## @return: client_advocate_data
## @inputs: []
client_advocate_data = glueContext.create_dynamic_frame.from_catalog(database = "advo-sbx-cluster",
                                                                     table_name = "adv_identity_client_advocate_data",
                                                                     transformation_ctx = "client_advocate_data")

## @type: DataSource
## @args: [database = "advo-sbx-cluster", table_name = "adv_identity_advocates", transformation_ctx = "advocates"]
## @return: advocates
## @inputs: []
advocates = glueContext.create_dynamic_frame.from_catalog(database = "advo-sbx-cluster",
                                                          table_name = "adv_identity_advocates",
                                                          transformation_ctx = "advocates")

## @type: DataSource
## @args: [database = "advo-sbx-cluster", table_name = "adv_identity_advocate_data", transformation_ctx = "advocates_data"]
## @return: advocates_data
## @inputs: []
advocates_data = glueContext.create_dynamic_frame.from_catalog(database = "advo-sbx-cluster",
                                                               table_name = "adv_identity_advocate_data",
                                                               transformation_ctx = "advocates_data")

## @type: DataSource
## @args: [database = "advo-sbx-cluster", table_name = "adv_identity_user_logins", transformation_ctx = "user_logins"]
## @return: user_logins
## @inputs: []
user_logins = glueContext.create_dynamic_frame.from_catalog(database = "advo-sbx-cluster",
                                                            table_name = "adv_identity_user_logins",
                                                            transformation_ctx = "user_logins")

## @type: DataSource
## @args: [database = "advo-sbx-cluster", table_name = "adv_identity_client_roles", transformation_ctx = "client_roles"]
## @return: client_roles
## @inputs: []
client_roles = glueContext.create_dynamic_frame.from_catalog(database = "advo-sbx-cluster",
                                                             table_name = "adv_identity_client_roles",
                                                             transformation_ctx = "client_roles")

## @type: DataSource
## @args: [database = "advo-sbx-cluster", table_name = "adv_identity_advocate_address", transformation_ctx = "advocate_address"]
## @return: advocate_address
## @inputs: []
advocate_address = glueContext.create_dynamic_frame.from_catalog(database = "advo-sbx-cluster",
                                                                 table_name = "adv_identity_advocate_address",
                                                                 transformation_ctx = "advocate_address")

## @type: DataSource
## @args: [database = "advo-sbx-cluster", table_name = "adv_identity_company", transformation_ctx = "company"]
## @return: company
## @inputs: []
company = glueContext.create_dynamic_frame.from_catalog(database = "advo-sbx-cluster",
                                                        table_name = "adv_identity_company",
                                                        transformation_ctx = "company")