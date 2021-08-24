#!/usr/bin/env python
# coding: utf-8

# In[1]:


import logging
import boto3
from botocore.exceptions import ClientError


# In[3]:


#Create S3 bucket - in a production systems you should avoid hard coding your AWS access and secret keys
AWS_ACCESS_KEY_ID = '<ACCESS_KEY>'
AWS_SECRET_ACCESS_KEY = '<SECRET_KEY>'

#Create a s3 client object
s3_client = boto3.client('s3', region_name ='eu-west-2',
                        aws_access_key_id = AWS_ACCESS_KEY_ID,
                        aws_secret_access_key = AWS_SECRET_ACCESS_KEY)

#Specify location of the s3 bucket using the Configuration parameter
location = {'LocationConstraint': 'eu-west-2'}

#Use the s3 client to create_bucket method to create a bucket
s3_client.create_bucket(Bucket = 'my-python-boto3-project',
                       CreateBucketConfiguration = location)


# In[4]:


# List Bucket on S3 with boto3 s3_client object 'list' method
bucket_list = s3_client.list_buckets()

for bucket in bucket_list['Buckets']:
    print(bucket['Name'])


# In[5]:


# Uploading files to a bucket with boto3 s3_client 'upload_file' method
file_name= 'Downloads/awesomecsv.csv'
bucket_name = 'my-python-boto3-project'
myfile = file_name

file_upload = s3_client.upload_file(file_name, bucket_name, myfile)


# In[8]:


# Uploading a file using the python file handler
with open(file_name, 'rb') as obj:
    s3_client.upload_fileobj(obj, bucket_name, 'Downloads/awesomecsv.clone.csv')


# In[9]:


# Upload file with ExtraArgs - enables you parse extra arguments when uploading a file
# Upload file with public read access
file_name= 'Downloads/awesomecsv.public.csv'
bucket_name = 'my-python-boto3-project'
myfile = file_name

file_upload = s3_client.upload_file(file_name, bucket_name, myfile,
                                   ExtraArgs={'ACL':'public-read'})


# In[10]:


# Downloading files
s3_client.download_file(bucket_name, 'Downloads/awesomecsv.clone.csv', 'Target/awesomecsv_download_target.csv')


# In[11]:


# Download and rename downloaded file using file handler (obj)
with open('Target/awesomecsv.copy.csv', 'wb') as obj:
    s3_client.download_fileobj(bucket_name, myfile, obj)


# In[15]:


# Multipart file transfers
# Multipart transfers is used when the file size exceeds the multipart_threshold attribute value.

from boto3.s3.transfer import TransferConfig

GByte = 1024**3

config = TransferConfig(multipart_threshold=5*GByte)

s3_client.upload_file(myfile, bucket_name, 'Downloads/awesomecsv.multipart.csv', Config=config)


# In[17]:


# Presigned URLs
#allow a user without AWS credentials or permissons to access an S3 object
# Presigned URLs grant temporary access for a limited period which is specified when the URL is generated
#the presigned URL can be enttered into a browser

url_response = s3_client.generate_presigned_url('get_object',
                                                Params={'Bucket': bucket_name,
                                                       'Key':myfile},
                                                ExpiresIn= 3600) #3600seconds

print(url_response)


# In[21]:


# Configuring Bucket polices
# Bucket policies are defined in JSON.

# Define a Bucket Policy
import json

bucket_name = 'my-python-boto3-project'
bucket_policy = {
    'Version':'2012-10-17',
    'Statement':[{
        'Sid':'AddPolicyPermission',
        'Effect':'Allow',
        'Principal':'*',
        'Action':['s3:GetObject'],
        'Resource':f'arn:aws:s3:::{bucket_name}/*'
    }]
}

bucket_policy = json.dumps(bucket_policy)
print(bucket_policy)
s3_client.put_bucket_policy(Bucket=bucket_name, Policy=bucket_policy)


# In[23]:


# Bucket policy - Retrieve Bucket policy
result = s3_client.get_bucket_policy(Bucket='my-python-boto3-project')
print(result)


# In[25]:


# Bucket Policy - Delete Bucket policy
s3_client.delete_bucket_policy(Bucket='my-python-boto3-project')


# In[26]:


# Setting Up a Bucket CORS Configuration 
# CORS - Cross Origin Resource Sharing enables clients from one domain access resources in another domain.

# You can use this method to allow sites access objects in your bucket.

# Set Bucket CORS
cors_configuration = {
    'CORSRules': [
        {'AllowedHeaders':['AUthorization'],
        'AllowedMethods':['GET','PUT'],
        'AllowedOrigins':["*"],
        'ExposeHeaders':['GET','PUT'],
        'MaxAgeSeconds':3000
        }
    ]
}

s3_client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration = cors_configuration
                         )


# In[27]:


# Retrieving a Bucket CORS Configuration
cors_response = s3_client.get_bucket_cors(Bucket='my-python-boto3-project')
print(cors_response['CORSRules'])


# In[ ]:




