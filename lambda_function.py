import json
import boto3
import email
import os
import logging


s3 = boto3.client('s3')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):

    # Define the source and destination S3 buckets
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    source_key = event['Records'][0]['s3']['object']['key']
    destination_bucket = 'my-s3-destination-bucket'  # Change to your destination bucket name
    
    # Get the raw email content from the source S3 bucket
    response = s3.get_object(Bucket=source_bucket, Key=source_key)
    raw_email_content = response['Body'].read().decode('utf-8')
    
    # Parse the raw email content
    msg = email.message_from_string(raw_email_content)

    # Initialize a list to store the CSV attachments
    csv_attachments = []
    
    for part in msg.walk():
        if part.get_content_type() == 'text/csv':
            csv_attachments.append(part)

    # Process and upload CSV attachments to the destination bucket
    for attachment in csv_attachments:
        attachment_name = attachment.get_filename()
        if attachment_name:
            destination_key = f"{attachment_name}"
            s3.put_object(Bucket=destination_bucket, Key=destination_key, Body=attachment.get_payload(decode=True))
            logger.info(f"Uploaded CSV attachment: {destination_key}")
    
    if not csv_attachments:
        logger.info("No CSV attachments found in the email.")
    
    return {
        'statusCode': 200,
        'body': json.dumps('SES Email received and processed!')
    }