import json
import boto3

def lambda_handler(event, context):
    """
    Lambda function to trigger glue etl job when input file lands in input S3 bucket.
    Args:
        event: containing the S3 bucket attributes
        context: context object
    """ 
    file_name = event['Records'][0]['s3']['object']['key']
    bucketName=event['Records'][0]['s3']['bucket']['name']
    print("File Name : ",file_name)
    print("Bucket Name : ",bucketName)
    glue=boto3.client('glue')
    response = glue.start_job_run(JobName = "extract_transfer_load", Arguments={"--VAL1":file_name,"--VAL2":bucketName})
    print("Lambda Invoke etl glue job")