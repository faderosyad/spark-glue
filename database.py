import sys

from ClientAdvocatesETL import glueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.shell import sqlContext
from pyspark.sql import *
from awsglue.context import GlueContext
from awsglue.job import Job

