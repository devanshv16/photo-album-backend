import boto3
import json
import logging
import os
from datetime import datetime
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION = 'us-east-1'
OPENSEARCH_ENDPOINT = os.environ.get('OPENSEARCH_ENDPOINT')  # set in env vars
INDEX_NAME = 'photos'

def get_opensearch_client():
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        REGION,
        'es',
        session_token=credentials.token
    )
    client = OpenSearch(
        hosts=[{'host': OPENSEARCH_ENDPOINT.replace('https://', ''), 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )
    return client

def lambda_handler(event, context):
    logger.info(f"Event: {json.dumps(event)}")
    
    s3 = boto3.client('s3')
    rekognition = boto3.client('rekognition', region_name=REGION)
    
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        
        logger.info(f"Processing: s3://{bucket}/{key}")
        
        # Detect labels using Rekognition
        rekognition_response = rekognition.detect_labels(
            Image={'S3Object': {'Bucket': bucket, 'Name': key}},
            MaxLabels=20,
            MinConfidence=70
        )
        
        labels = [label['Name'].lower() for label in rekognition_response['Labels']]
        logger.info(f"Rekognition labels: {labels}")
        
        # Get S3 object metadata for custom labels
        head_response = s3.head_object(Bucket=bucket, Key=key)
        metadata = head_response.get('Metadata', {})
        custom_labels_raw = metadata.get('customlabels', '')  # x-amz-meta-customlabels
        
        if custom_labels_raw:
            custom_labels = [l.strip().lower() for l in custom_labels_raw.split(',') if l.strip()]
            labels.extend(custom_labels)
            logger.info(f"Custom labels added: {custom_labels}")
        
        # Remove duplicates
        labels = list(set(labels))
        
        # Create JSON document
        timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        photo_doc = {
            'objectKey': key,
            'bucket': bucket,
            'createdTimestamp': timestamp,
            'labels': labels
        }
        
        logger.info(f"Indexing document: {json.dumps(photo_doc)}")
        
        # Index into OpenSearch
        es = get_opensearch_client()
        
        # Create index if it doesn't exist
        if not es.indices.exists(index=INDEX_NAME):
            es.indices.create(
                index=INDEX_NAME,
                body={
                    "mappings": {
                        "properties": {
                            "objectKey": {"type": "keyword"},
                            "bucket": {"type": "keyword"},
                            "createdTimestamp": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss"},
                            "labels": {"type": "keyword"}
                        }
                    }
                }
            )
        
        doc_id = key.replace('/', '-').replace('.', '-')
        response = es.index(index=INDEX_NAME, id=doc_id, body=photo_doc)
        logger.info(f"OpenSearch response: {response}")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Photos indexed successfully')
    }