import json
import boto3
import os
import logging
from datetime import datetime, timezone, date

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sqs = boto3.client('sqs')
ses = boto3.client('ses', region_name='us-east-1')
dynamodb = boto3.resource('dynamodb')

STATE_TABLE = dynamodb.Table(os.environ['STATE_TABLE_NAME'])
DAILY_CAP_TABLE = dynamodb.Table(os.environ['DAILY_CAP_TABLE_NAME'])
CONFIG_TABLE = dynamodb.Table(os.environ['CONFIG_TABLE_NAME'])
FOLLOWUP_QUEUE_URL = os.environ['FOLLOWUP_QUEUE_URL']
FROM_EMAIL = os.environ['FROM_EMAIL']
JOURNEY_ID = 'abandoned_checkout_mvp'
MAX_MESSAGES_PER_TICK = 10


def lambda_handler(event, context):
    config = _get_journey_config()
    if not config or not config.get('isActive'):
        return

    messages = _receive_messages()
    logger.info(f"Received {len(messages)} messages from follow-up queue")

    for msg in messages:
        try:
            body = json.loads(msg['Body'])
            receipt_handle = msg['ReceiptHandle']
            _process_followup(body, receipt_handle, config)
        except Exception as e:
            logger.error(f"Error processing follow-up message: {e}", exc_info=True)
            # Leave message in queue for retry — do not delete


def _process_followup(task, receipt_handle, config):
    user_id = task['userId']
    cart_id = task['cartId']
    send_at = datetime.fromisoformat(task['sendAt'].replace('Z', '+00:00'))
    now = datetime.now(timezone.utc)

    # Not yet time to send — leave in queue
    #if now < send_at:
    #    logger.info(f"Not yet send time for user={user_id}. Scheduled={send_at.isoformat()}")
    #    return

    # Load fresh state from DynamoDB
    state = _get_state(user_id, cart_id)
    if not state:
        logger.warning(f"No state found for user={user_id} cart={cart_id} — deleting task")
        _delete_message(receipt_handle)
        return

    # --- Send-time cancellation checks ---

    # A2: Purchase completed since queuing
    if state.get('cartStatus') == 'purchased':
        _cancel_and_delete(state, receipt_handle, 'purchase_completed_before_send')
        return

    # A3: Cart emptied since queuing
    if state.get('cartStatus') == 'emptied':
        _cancel_and_delete(state, receipt_handle, 'cart_emptied_before_send')
        return

    # A3 / C2: Cart value dropped below threshold at send time
    cart_value = float(state.get('cartValue', 0))
    threshold = float(config.get('cartValueThreshold', 25))
    if cart_value < threshold:
        _cancel_and_delete(state, receipt_handle, f'cart_value_below_threshold_at_send_{cart_value}')
        return

    # B1 / C1: A newer cart exists for same dealer — cancel older cart email
    newer_cart = _get_newer_cart_for_dealer(user_id, cart_id, state.get('dealerCode'), state.get('cartLastUpdatedAt'))
    if newer_cart:
        _cancel_and_delete(state, receipt_handle, f'newer_cart_exists_{newer_cart["cartId"]}')
        return

    # B2 / D2: Daily email cap — one email per user per calendar day
    today = date.today().isoformat()
    if _daily_cap_reached(user_id, today):
        _cancel_and_delete(state, receipt_handle, 'daily_cap_reached')
        return

    # Consent re-check at send time
    if state.get('cartConsent') != 'given':
        _cancel_and_delete(state, receipt_handle, f"consent_{state.get('cartConsent')}_at_send")
        return

    # --- All checks passed — send the email ---
    success = _send_email(user_id, state, task)

    if success:
        now_str = now.isoformat()
        state['journeyStatus'] = 'email_sent'
        state['followupSentAt'] = now_str
        state['lastEmailSentDate'] = today
        STATE_TABLE.put_item(Item=state)

        # Write daily cap record
        _write_daily_cap(user_id, today, cart_id, state.get('dealerCode'), now_str)

        _delete_message(receipt_handle)
        logger.info(f"Email sent for user={user_id} cart={cart_id}")
    else:
        logger.error(f"SES send failed for user={user_id} — leaving in queue for retry")


def _send_email(user_id, state, task):
    """
    Send via SES. In MVP we send to a verified test address.
    In production this would resolve the actual email from userId
    via a secure lookup service — no PII stored in this pipeline.
    """
    try:
        # MVP: send to FROM_EMAIL for testing
        # Production: replace with secure email resolution from userId
        recipient = FROM_EMAIL

        subject = f"You left something in your cart at Toyota Parts Center"
        body_text = (
            f"Hi,\n\n"
            f"You have items worth ${state.get('cartValue', 0):.2f} "
            f"waiting in your cart at {state.get('dealerCode')}.\n\n"
            f"Complete your purchase at autoparts.toyota.com\n\n"
            f"Cart reference: {task.get('cartId')}\n"
        )

        ses.send_email(
            Source=FROM_EMAIL,
            Destination={'ToAddresses': [recipient]},
            Message={
                'Subject': {'Data': subject, 'Charset': 'UTF-8'},
                'Body': {'Text': {'Data': body_text, 'Charset': 'UTF-8'}}
            }
        )
        return True
    except Exception as e:
        logger.error(f"SES send_email failed: {e}")
        return False


# ---------------------------------------------------------------------------
# Send-time check helpers
# ---------------------------------------------------------------------------

def _get_newer_cart_for_dealer(user_id, current_cart_id, dealer_code, current_cart_updated_at):
    """
    Scenario B1/C1: Check if a newer cart exists for same user+dealer.
    Queries all carts for this userId, then filters in Python — avoids
    DynamoDB restriction on using sort key in FilterExpression.
    """
    from boto3.dynamodb.conditions import Key, Attr
    resp = STATE_TABLE.query(
        KeyConditionExpression=Key('userId').eq(user_id),
        FilterExpression=(
            Attr('dealerCode').eq(dealer_code) &
            Attr('cartLastUpdatedAt').gt(current_cart_updated_at)
        )
    )
    items = resp.get('Items', [])
    # Filter out the current cart in Python, not in DynamoDB
    newer = [i for i in items if i.get('cartId') != current_cart_id]
    return newer[0] if newer else None

def _daily_cap_reached(user_id, today):
    resp = DAILY_CAP_TABLE.get_item(Key={'userId': user_id, 'emailDate': today})
    return 'Item' in resp


def _write_daily_cap(user_id, today, cart_id, dealer_code, sent_at):
    from datetime import timedelta
    ttl = int((datetime.now(timezone.utc) + timedelta(days=2)).timestamp())
    DAILY_CAP_TABLE.put_item(Item={
        'userId': user_id,
        'emailDate': today,
        'emailSentAt': sent_at,
        'cartId': cart_id,
        'dealerCode': dealer_code,
        'ttlExpiry': ttl
    })


def _cancel_and_delete(state, receipt_handle, reason):
    state['journeyStatus'] = 'email_cancelled'
    state['followupCancelledAt'] = datetime.now(timezone.utc).isoformat()
    state['followupCancelReason'] = reason
    STATE_TABLE.put_item(Item=state)
    _delete_message(receipt_handle)
    logger.info(f"Follow-up cancelled at send time: {reason} for cart {state.get('cartId')}")


# ---------------------------------------------------------------------------
# SQS helpers
# ---------------------------------------------------------------------------

def _receive_messages():
    resp = sqs.receive_message(
        QueueUrl=FOLLOWUP_QUEUE_URL,
        MaxNumberOfMessages=MAX_MESSAGES_PER_TICK,
        WaitTimeSeconds=5,
        MessageAttributeNames=['All']
    )
    return resp.get('Messages', [])


def _delete_message(receipt_handle):
    sqs.delete_message(QueueUrl=FOLLOWUP_QUEUE_URL, ReceiptHandle=receipt_handle)


# ---------------------------------------------------------------------------
# DynamoDB helpers
# ---------------------------------------------------------------------------

def _get_state(user_id, cart_id):
    resp = STATE_TABLE.get_item(Key={'userId': user_id, 'cartId': cart_id})
    return resp.get('Item')


def _get_journey_config():
    resp = CONFIG_TABLE.get_item(Key={'journeyId': JOURNEY_ID})
    return resp.get('Item')
