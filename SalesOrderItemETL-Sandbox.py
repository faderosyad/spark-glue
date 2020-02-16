import sys
import os
import datetime
from datetime import datetime, timedelta
from awsglue.transforms import *
from awsglue.transforms.apply_mapping import ApplyMapping
from awsglue.transforms.field_transforms import SelectFields
from awsglue.transforms.resolve_choice import ResolveChoice
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark.sql import *
from pyspark.sql.types import *
# from pyspark.sql.functions import lit


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME',
                                     'loki_start',
                                     'loki_end',
                                     'client_identifier'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

LOKI_START = args['loki_start']
LOKI_END = args['loki_end']
clientIdentifier = args['client_identifier']

nowDateTime = datetime.now() + timedelta(hours=7)

# Import table from OLTP
salesOrders = glueContext.create_dynamic_frame.from_catalog(database = "advo-sbx-cluster",
                                                            table_name = "adv_item_sales_orders",
                                                            transformation_ctx = "salesOrders").toDF()
salesOrderItems = glueContext.create_dynamic_frame_from_catalog(database = "advo-sbx-cluster",
                                                                table_name =,
                                                                transformation_ctx = "salesOrderItems").toDF()
clientProduct = glueContext.create_dynamic_frame_from_catalog(database = "advo-sbx-cluster",
                                                              table_name =,
                                                              transformation_ctx = "clientProduct").toDF()
advocate = glueContext.create_dynamic_frame_from_catalog(database = "advo-sbx-cluster",
                                                         table_name =,
                                                         transformation_ctx = "advocate").toDF()
visit = glueContext.create_dynamic_frame_from_catalog(database = "advo-sbx-cluster",
                                                      table_name = ,
                                                      transformation_ctx = "visit").toDF()
salesOrderItemDiscount = glueContext.create_dynamic_frame_from_catalog(database = "advo-sbx-cluster",
                                                                       table_name =,
                                                                       transformation_ctx = "salesOrderItemDiscount").toDF()

clientWorkgroupProduct = glueContext.create_dynamic_frame_from_catalog(database = "advo-sbx-cluster",
                                                                       table_name =,
                                                                       transformation_ctx = "clientWorkgroupProduct").toDF()

# Strategies
# 1. Learn to create special dataframe that just for mapping only - Done
# 2. Learn to put data in row that will be unity() to dataframe
## 2a. Learn how to create a list that will be basic for dataframe
## 2b. Learn how to map a cell data to so it can be a variable
## 2c. Learn how to manipulate data in that list
# 3. Repeat that process to another row

# Create dataframe to be data pull for the a whole workflow
dataInput = [StructField("client_id", IntegerType, False),
             StructField("sales_order_no", StringType, False),
             StructField("item_seq", StringType, True),
             StructField("sales_channel", StringType, True),
             StructField("sales_id", IntegerType, True),
             StructField("sales_name", StringType, True),
             StructField("sales_channel_ref_id", StringType, True),
             StructField("customer_id", IntegerType, True),
             StructField("customer_name", StringType, True),
             StructField("customer_ref_id", StringType, True),
             StructField("product_code", StringType, True),
             StructField("product_name", StringType, True),
             StructField("status", StringType, True),
             StructField("client_ref_id", IntegerType, True),
             StructField("annotation", StringType, True),
             StructField("fulfiller_id", IntegerType, True),
             StructField("fulfiller_name", StringType, True),
             StructField("fulfilled_time", DateType, True),
             StructField("quantity", IntegerType, True),
             StructField("fulfilled_quantity", IntegerType, True),
             StructField("price", DoubleType, True),
             StructField("discount", DoubleType, True),
             StructField("discount_lvl_sales_order_item", StringType, True),
             StructField("discount_lvl_subtotal", StringType, True),
             StructField("discount_lvl_sales_order", StringType, True),
             StructField("discount_item", DoubleType, True),
             StructField("discount_order_subtotal", DoubleType, True),
             StructField("discount_revenue_subtotal", DoubleType, True),
             StructField("discount_order_total", DoubleType, True),
             StructField("discount_revenue_total", DoubleType, True),
             StructField("tax", DoubleType, True),
             StructField("tax_sales_order", StringType, True),
             StructField("total_sales_order", StringType, True),
             StructField("date_index", IntegerType, True),
             StructField("month_index", IntegerType, True),
             StructField("creation_time", DateType, True),
             StructField("sales_order_item_creation_time", DateType, True),
             StructField("sales_order_creation_time", DateType, True),
             StructField("loki_index", IntegerType, True),
             StructField("is_active", StringType, True),
             StructField("data", StringType, True),
             StructField("revision_id", IntegerType, True),
             StructField("submitted_time", IntegerType, True),
             StructField("sales_ref_id", StringType, True),
             StructField("sales_phone_number", StringType, True),
             StructField("customer_sales_group_id", IntegerType, True),
             StructField("customer_address", StringType, True),
             StructField("customer_regency_name", StringType, True),
             StructField("distributor_name", StringType, True),
             StructField("distributor_address", StringType, True),
             StructField("distributor_regency_name", StringType, True),
             StructField("total_discount_amount", DoubleType, True),
             StructField("longitude", DoubleType, True),
             StructField("latitude", DoubleType, True),
             StructField("is_merchandise", StringType, True),
             StructField("content_type", StringType, True),
             StructField("content_per_unit", DoubleType, True),
             StructField("sales_workgroup", StringType, True),
             StructField("product_owner", StringType, True),
             StructField("nett_amount_with_tax", DoubleType, True),
             StructField("nett_amount_without_tax", DoubleType, True),
             StructField("gross_amount", DoubleType, True),
             StructField("gross_per_line", DoubleType, True),
             StructField("nett_per_line", DoubleType, True),
             StructField("nett_order_per_line", DoubleType, True),
             StructField("brand_id", IntegerType, True),
             StructField("brand_name", StringType, True),
             StructField("product_group_id", IntegerType, True),
             StructField("product_group_name", StringType, True),
             StructField("product_workgroup_code", StringType, True),
             StructField("product_workgroup_name", StringType, True)]

schema = StructType(dataInput)
dataframeInput = spark.createDataFrame(sc.emptyRDD(), schema)

##Example of how adding row
# newRow = spark.createDataFrame([(clientId, SalesOrderNo, lalala....)])
# appended = dataframeInput.union(newRow)

# Create a list from all OLTP data source that repeated for all base table row
baseCounter = salesOrders.count()

# List structure example
## dataRow = [clientID, salesOrderNo, itemSeq
### problem = how to mapping one variable to a dataframe
### solve = set variable as dataframe filter variable

## Iterate row filling
clientId = salesOrders.client_id[0]
salesOrderNo = salesOrders.sales_order_no[0]

newRow = spark.createDataFrame([(clientId, salesOrderNo, isnull, isnull)])
dataframeInput = dataframeInput.union(newRow)

applymapping1 = ApplyMapping.apply(frame = dataframeInput,
                                   mappings = [("sales_channel_ref_id", "string", "sales_channel_ref_id", "string"),
                                               ("sales_channel", "string", "sales_channel", "string"),
                                               ("discount", "double", "discount", "double"),
                                               ("tax", "double", "tax", "double"),
                                               ("submitted_time", "long", "submitted_time", "long"),
                                               ("sales_order_no", "string", "sales_order_no", "string"),
                                               ("client_id", "int", "client_id", "int"),
                                               ("client_ref_id", "string", "client_ref_id", "string"),
                                               ("fulfiller_id", "int", "fulfiller_id", "int"),
                                               ("fulfilled_time", "timestamp", "fulfilled_time", "timestamp"),
                                               ("creation_time", "timestamp", "sales_order_creation_time", "timestamp"),
                                               ("customer_id", "int", "customer_id", "int"),
                                               ("sales_id", "int", "sales_id", "int"),
                                               ("status", "string", "status", "string")],
                                   transformation_ctx = "applymapping1")

selectfields2 = SelectFields.apply(frame = applymapping1,
                                   paths = ["creation_time",
                                            "discount_lvl_subtotal",
                                            "discount_revenue_subtotal",
                                            "data",
                                            "product_owner",
                                            "item_seq",
                                            "sales_channel_ref_id",
                                            "discount",
                                            "nett_amount_without_tax",
                                            "fulfilled_quantity",
                                            "gross_amount",
                                            "client_id",
                                            "discount_order_total",
                                            "sales_order_item_creation_time",
                                            "discount_order_subtotal",
                                            "date_index",
                                            "month_index",
                                            "sales_ref_id",
                                            "price",
                                            "customer_regency_name",
                                            "nett_amount_with_tax",
                                            "longitude",
                                            "customer_address",
                                            "nett_order_per_line",
                                            "product_workgroup_name",
                                            "sales_channel",
                                            "tax",
                                            "tax_sales_order",
                                            "brand_name",
                                            "product_name",
                                            "brand_id",
                                            "client_ref_id",
                                            "sales_name",
                                            "customer_ref_id",
                                            "status",
                                            "latitude",
                                            "total_discount_amount",
                                            "product_code",
                                            "sales_phone_number",
                                            "sales_order_no",
                                            "fulfilled_time",
                                            "content_type",
                                            "distributor_regency_name",
                                            "product_group_name",
                                            "is_merchandise",
                                            "sales_id",
                                            "discount_item",
                                            "revision_id",
                                            "annotation",
                                            "discount_lvl_sales_order",
                                            "total_sales_order",
                                            "nett_per_line",
                                            "quantity",
                                            "is_active",
                                            "sales_order_creation_time",
                                            "fulfiller_id_name",
                                            "discount_revenue_total",
                                            "submitted_time",
                                            "customer_sales_group_id",
                                            "product_group_id",
                                            "product_workgroup_code",
                                            "fulfiller_id",
                                            "discount_lvl_sales_order_item",
                                            "distributor_address",
                                            "gross_per_line",
                                            "distributor_name",
                                            "customer_name",
                                            "customer_id",
                                            "content_per_unit",
                                            "loki_index",
                                            "sales_workgroup"],
                                   transformation_ctx = "selectfields2")

resolvechoice3 = ResolveChoice.apply(frame = selectfields2,
                                     choice = "MATCH_CATALOG",
                                     database = "adv-sbx-ddl",
                                     table_name = "advotics_dw_dw_raw_sales_order_items",
                                     transformation_ctx = "resolvechoice3")

resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3,
                                     choice = "make_cols",
                                     transformation_ctx = "resolvechoice4")

datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4,
                                                         database = "adv-sbx-ddl",
                                                         table_name = "advotics_dw_dw_raw_sales_order_items",
                                                         transformation_ctx = "datasink5")

job.commit()