from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkConf
import os

conf = (SparkConf().set("spark.executor.extraJavaOptions","-Dcom.amazonaws.services.s3.enableV4=true")
    .set("spark.driver.extraJavaOptions","-Dcom.amazonaws.services.s3.enableV4=true"))
print("8 ===>")
scT=SparkContext(conf=conf)
scT.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
print("11 ===>")

access_key_id = 'AKIAWWJR22JASYNFCL7C',
secret_access_key = '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',
region = 'ap-south-1'
bucketname = 'myfirstbucketqwerty'
keyfile ='MyFirstProject/medical_hospital_claim.csv'

hadoopConf = scT._jsc.hadoopConfiguration()
print(hadoopConf)
hadoopConf.set("fs.s3a.awsAccessKeyId",'AKIAWWJR22JASYNFCL7C' )
hadoopConf.set("fs.s3a.awsSecretAccessKey", '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')
hadoopConf.set("fs.s3a.endpoint", "ap-south-1.amazonaws.com")
hadoopConf.set("com.amazonaws.services.s3a.enableV4", "true")
hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '-- packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell'

#spark configuration
conf = SparkConf().set('spark.executor.extraJavaOptions','-Dcom.amazonaws.services.s3.enableV4=true'). \
 set('spark.driver.extraJavaOptions','-Dcom.amazonaws.services.s3.enableV4=true'). \
 setAppName('pyspark_aws').setMaster('local[*]')

# os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell"
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-pom:1.11.538,org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell'


print("====> 26")
# sc = SparkContext(conf=conf)
# scT._jsc.hadoopConfiguration().set('fs.s3a.access.key', 'AKIAWWJR22JASYNFCL7C')
# scT._jsc.hadoopConfiguration().set('fs.s3a.secret.key', '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')

# spark = SparkSession.builder.master("local").config(conf=SparkConf()).getOrCreate()
from pyspark.sql import SQLContext

sqlContext = SQLContext(scT)
sql = SparkSession(scT)
print("===> 44")
# inputFile = sql.read.csv('s3://myfirstbucketqwerty/MyFirstProject/medical_hospital_claim.csv')
# print(inputFile)


# hadoopConf = scT.hadoopConfiguration()
hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
# hadoopConf.set("fs.s3.awsAccessKeyId", access_key_id)
# hadoopConf.set("fs.s3.awsSecretAccessKey", secret_access_key)
jobInput = scT.textFile('s3://myfirstbucketqwerty/MyFirstProject/medical_hospital_claim.csv')

print(jobInput)


# sqlContext.createDataFrame(inputFile)
# df = inputFile.toDF()
# df.show()

# sql = SparkSession(scT)
# csv_df = sql.read.csv('s3://myfirstbucketqwerty/MyFirstProject/medical_hospital_claim.csv')
# print(csv_df)