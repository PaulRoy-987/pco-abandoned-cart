import json
import boto3
import os
import logging
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sqs = boto3.client('sqs')
RAW_EVENTS_URL = os.environ['RAW_EVENTS_QUEUE_URL']

VALID_EVENT_TYPES = {
    'page_view', 'add_to_cart', 'remove_from_cart',
    'view_cart', 'checkout', 'purchase',
    'consent_given', 'consent_revoked'
}

CART_REQUIRED_FIELDS = {'cartId', 'cartValue', 'cartConsent', 'dealerCode'}
CART_EVENT_TYPES = {
    'add_to_cart', 'remove_from_cart', 'view_cart',
    'checkout', 'purchase', 'consent_given', 'consent_revoked'
}


def lambda_handler(event, context):
    try:
        body = json.loads(event.get('body', '{}'))
    except json.JSONDecodeError:
        return _response(400, 'Invalid JSON body')

    # Basic required field check
    if 'eventType' not in body or body['eventType'] not in VALID_EVENT_TYPES:
        return _response(400, 'Invalid or missing eventType')

    if 'userId' not in body or not body['userId']:
        return _response(400, 'Missing userId')

    # Cart events need extra fields
    if body['eventType'] in CART_EVENT_TYPES:
        missing = CART_REQUIRED_FIELDS - set(body.keys())
        if missing:
            return _response(400, f"Missing fields for cart event: {missing}")

    # Stamp server-side times
    now = datetime.now(timezone.utc).isoformat()
    body['receivedAt'] = now

    # Preserve original client timestamp for tracing/debugging
    # Use server-side timestamp for all pipeline calculations
    if 'eventTimestamp' in body:
        body['clientEventTimestamp'] = body['eventTimestamp']
    body['eventTimestamp'] = now

    sqs.send_message(
        QueueUrl=RAW_EVENTS_URL,
        MessageBody=json.dumps(body)
    )

    logger.info(
        f"Queued event: {body['eventType']} for user {body['userId']} "
        f"| serverTime={now} "
        f"| clientTime={body.get('clientEventTimestamp', 'not provided')}"
    )
    return _response(200, 'Event received')


def _response(status_code, message):
    return {
        'statusCode': status_code,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps({'message': message})
    }