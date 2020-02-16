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

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

salesOrderItems = glueContext.create_dynamic_frame.from_catalog(database="advo-sbx-cluster",
                                                                table_name="adv_item_sales_order_items",
                                                                transformation_ctx="salesOrderItems").toDF()

dataInput = StructType([StructField("client_id", IntegerType(), False),
                        StructField("sales_order_no", StringType(), False),
                        StructField("item_seq", IntegerType(), False),
                        StructField("product_code", StringType(), True),
                        StructField("quantity", IntegerType(), True),
                        StructField("fulfilled_quantity", IntegerType(), True),
                        StructField("is_active", StringType(), True),
                        StructField("price", DoubleType(), True),
                        StructField("discount", DoubleType(), True)])

dataframeInput = spark.createDataFrame(sc.emptyRDD(), dataInput)

baseCounter = salesOrderItems.count()
i = 0
while (i <= 100):
    clientId = salesOrderItems.client_id[i]
    salesOrderNo = salesOrderItems.sales_order_no[i]
    itemSeq = salesOrderItems.seq[i]
    productCode = salesOrderItems.product_code[i]
    quantity = salesOrderItems.quantity[i]
    fulfilledQuantity = salesOrderItems.fulfilled_quantity[i]
    isActive = salesOrderItems.is_active[i]
    price = salesOrderItems.price[i]
    discount = salesOrderItems.discount[i]

    newRow = spark.createDataFrame([(clientId, salesOrderNo, itemSeq, productCode, quantity, fulfilledQuantity,
                                     isActive, price, discount)])
    dataframeInput = dataframeInput.union(newRow)
    i += 1

applymapping1 = ApplyMapping.apply(frame=dataframeInput,
                                   mappings=[("sales_order_no", "string", "sales_order_no", "string"),
                                             ("client_id", "int", "client_id", "int"),
                                             ("item_seq", "int", "item_seq", "int")],
                                   transformation_ctx="applymapping1")

selectfields2 = SelectFields.apply(frame=applymapping1,
                                   paths=["sales_name", "customer_ref_id", "item_seq", "sales_channel_ref_id",
                                          "sales_channel", "customer_name", "customer_id", "sales_order_no", "sales_id",
                                          "client_id"],
                                   transformation_ctx="selectfields2")

resolvechoice3 = ResolveChoice.apply(frame=selectfields2, choice="MATCH_CATALOG", database="adv-sbx-ddl",
                                     table_name="advotics_dw_dw_raw_sales_order_items_glue_exp",
                                     transformation_ctx="resolvechoice3")

resolvechoice4 = ResolveChoice.apply(frame=resolvechoice3, choice="make_cols", transformation_ctx="resolvechoice4")

datasink5 = glueContext.write_dynamic_frame.from_catalog(frame=resolvechoice4, database="adv-sbx-ddl",
                                                         table_name="advotics_dw_dw_raw_sales_order_items_glue_exp",
                                                         transformation_ctx="datasink5")

job.commit()
