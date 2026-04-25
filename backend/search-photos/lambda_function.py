import boto3
import json
import logging
import os
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION = 'us-east-1'
OPENSEARCH_ENDPOINT = os.environ.get('OPENSEARCH_ENDPOINT')
INDEX_NAME = 'photos'
BOT_ID = os.environ.get('LEX_BOT_ID')
BOT_ALIAS_ID = os.environ.get('LEX_BOT_ALIAS_ID')
PHOTOS_BUCKET = os.environ.get('PHOTOS_BUCKET')

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

def get_keywords_from_lex(query):
    """Send query to Lex and extract slot values as keywords."""
    lex = boto3.client('lexv2-runtime', region_name=REGION)
    
    try:
        response = lex.recognize_text(
            botId=BOT_ID,
            botAliasId=BOT_ALIAS_ID,
            localeId='en_US',
            sessionId='search-session-001',
            text=query
        )
        logger.info(f"Lex response: {json.dumps(response, default=str)}")
        
        keywords = []
        interpretations = response.get('interpretations', [])
        
        for interpretation in interpretations:
            intent = interpretation.get('intent', {})
            if intent.get('name') == 'SearchIntent':
                slots = intent.get('slots', {})
                for slot_name, slot_value in slots.items():
                    if slot_value and slot_value.get('value'):
                        val = slot_value['value'].get('interpretedValue', '')
                        if val:
                            keywords.append(val.lower().strip())
                break
        
        logger.info(f"Extracted keywords: {keywords}")
        return keywords
        
    except Exception as e:
        logger.error(f"Lex error: {str(e)}")
        # Fallback: use raw query as keywords
        return [w.lower().strip() for w in query.split() if len(w) > 2]

def search_photos(keywords):
    """Search OpenSearch for photos matching any of the keywords."""
    es = get_opensearch_client()
    
    query = {
        "query": {
            "terms": {
                "labels": keywords
            }
        }
    }
    
    logger.info(f"OpenSearch query: {json.dumps(query)}")
    
    response = es.search(index=INDEX_NAME, body=query, size=20)
    logger.info(f"OpenSearch response hits: {response['hits']['total']}")
    
    return response['hits']['hits']

def lambda_handler(event, context):
    logger.info(f"Event: {json.dumps(event)}")
    
    # Get query from query string parameters
    query_params = event.get('queryStringParameters') or {}
    query = query_params.get('q', '').strip()
    
    if not query:
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Content-Type': 'application/json'
            },
            'body': json.dumps({'results': []})
        }
    
    logger.info(f"Search query: {query}")
    
    # Step 1: Get keywords from Lex
    keywords = get_keywords_from_lex(query)
    
    if not keywords:
        logger.info("No keywords from Lex, returning empty results")
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Content-Type': 'application/json'
            },
            'body': json.dumps({'results': []})
        }
    
    # Step 2: Search OpenSearch
    hits = search_photos(keywords)
    
    # Step 3: Format results
    results = []
    s3 = boto3.client('s3')
    
    for hit in hits:
        source = hit['_source']
        bucket = source.get('bucket', PHOTOS_BUCKET)
        key = source.get('objectKey', '')
        
        # Generate a pre-signed URL so the frontend can display the image
        url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket, 'Key': key},
            ExpiresIn=3600
        )
        
        results.append({
            'url': url,
            'labels': source.get('labels', []),
            'objectKey': key,
            'bucket': bucket
        })
    
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Content-Type': 'application/json'
        },
        'body': json.dumps({'results': results})
    }