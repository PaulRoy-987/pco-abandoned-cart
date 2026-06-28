import json
import boto3
import os
import logging
import hashlib
from collections import defaultdict

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sqs = boto3.client('sqs')
PROCESSING_QUEUE_URL = os.environ['PROCESSING_QUEUE_URL']


def lambda_handler(event, context):
    grouped = defaultdict(list)

    for record in event['Records']:
        try:
            body = json.loads(record['body'])
            user_id = body.get('userId')
            cart_id = body.get('cartId', 'no_cart')

            if not user_id:
                logger.warning(f"Skipping record with no userId")
                continue

            group_key = f"{user_id}#{cart_id}"
            grouped[group_key].append(body)

        except Exception as e:
            logger.error(f"Failed to parse record: {e}")
            continue

    logger.info(f"Aggregated {len(event['Records'])} events into {len(grouped)} user+cart groups")

    for group_key, events in grouped.items():
        user_id, cart_id = group_key.split('#', 1)

        events_sorted = sorted(
            events,
            key=lambda e: e.get('eventTimestamp', e.get('receivedAt', ''))
        )

        message = {
            'userId': user_id,
            'cartId': cart_id,
            'eventCount': len(events_sorted),
            'events': events_sorted,
            'windowClosedAt': events_sorted[-1].get('receivedAt', '')
        }

        dedup_id = hashlib.md5(
            f"{group_key}#{message['windowClosedAt']}".encode()
        ).hexdigest()

        sqs.send_message(
            QueueUrl=PROCESSING_QUEUE_URL,
            MessageBody=json.dumps(message),
            MessageGroupId=user_id,
            MessageDeduplicationId=dedup_id
        )

    return {'statusCode': 200}