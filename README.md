# s3-redshift-lambda-loader

Redshift S3 Files Lambda Loader with KMS Encryption

Requirements:

1.	Redshift cluster and a redshift table

2.	Dynamodb table

3.	KMS key

4.	S3 bucket



1.	Encrypt your database userid, password, access key and secret key using the tool (ConfigKeysGenerator.java) provided to create the configuration file. A file redl0ader_ciphertext.txt is created in the root directory

2.	Create an S3 bucket with below prefixes (config, incoming, processed, failed): All files will come into the incoming folder and the Lambda function is configured to watch this S3 prefix and will trigger on arrival of files into this location. Processed files are swept into the processed S3 prefix while failed files are swept into the failed S3 prefix.\

3.	Upload the configuration file (redl0ader_ciphertext.txt) created from 1 above to an S3 bucket location

4.	Clone the repository git clone 

5.	Create a dynamodb table

6.	Make appropriate changes to the Loader class: replace the following with your own values

a.	Loader.BUCKET_NAME

b.	Loader.CONFIG_PREFIX

c.	Loader.KEY_ARN

d.	DDBHelper.TABLE_NAME

e.	DBConnection.DB_URL

7.	Compile and create a deployment package by following the link below

8.	Upload deployment package to create a Lambda function

9.	Test your Lambda function



