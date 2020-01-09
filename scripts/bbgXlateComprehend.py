import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## @type: DataSource
## @args: [database = "default", table_name = "news", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "default", table_name = "news", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("classnum", "int", "classnum", "int"), ("headline", "string", "headline", "string"), ("timeofarrival", "string", "timeofarrival", "string"), ("wireid", "int", "wireid", "int"), ("wirename", "string", "wirename", "string"), ("storygroupid", "string", "storygroupid", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("classnum", "int", "classnum", "int"), ("headline", "string", "headline", "string"), ("timeofarrival", "string", "timeofarrival", "string"), ("wireid", "int", "wireid", "int"), ("wirename", "string", "wirename", "string"), ("storygroupid", "string", "storygroupid", "string")], transformation_ctx = "applymapping1")

s0 = DynamicFrame.fromDF(applymapping1.toDF().coalesce(1), glueContext, "s0")

s0.printSchema()
print s0.count()

import boto3
def xlate(inp, srclang='auto', tgtlang='en'):
    translate = boto3.client(service_name='translate', region_name='us-east-1', use_ssl=True)
    output = " "
    try:
        result=translate.translate_text(Text=inp, 
        SourceLanguageCode=srclang, TargetLanguageCode=tgtlang)
        output = result.get('TranslatedText')
    except Exception as e:
        #logger.error(response)
        # raise Exception
        print("[ErrorMessage]: " + str(e))
    return output

from pyspark.sql.functions import udf, expr, concat, col
from pyspark.sql.types import StringType
xlate_udf_str = udf(lambda z: xlate(z), StringType())
#classnum,headline,timeofarrival,wireid,wirename,storygroupid
print("Xlate begin")
s1 = DynamicFrame.fromDF(s0.toDF().select('classnum','timeofarrival','wireid','wirename','storygroupid','headline',
              xlate_udf_str('headline').alias('xlated_headline')),
              glueContext, "s1")

print("Xlate end")

def sentiment(inp):
    client = boto3.client(service_name='comprehend', region_name='us-east-1', use_ssl=True)
    result=client.detect_sentiment(Text=inp, 
                                   LanguageCode='en')
    return result.get('Sentiment')
sentiment_udf_str = udf(lambda z: sentiment(z), StringType())  

s2 = DynamicFrame.fromDF(s1.toDF().select('classnum','timeofarrival','wireid','wirename','storygroupid','headline','xlated_headline',
              sentiment_udf_str('xlated_headline').alias('sentiment')),
              glueContext, "s2")

s2.printSchema()
              
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://mod-da6d820750784dd7-simplebucket-1jeg10o4329yx/data/xlated/"}, format = "csv", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = applymapping1]
datasink2 = glueContext.write_dynamic_frame.from_options(frame = s2, connection_type = "s3", connection_options = {"path": "s3://mod-da6d820750784dd7-simplebucket-1jeg10o4329yx/data/xlated/"}, format = "csv", transformation_ctx = "datasink2")

job.commit()

