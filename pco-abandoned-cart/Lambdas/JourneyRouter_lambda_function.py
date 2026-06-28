import json
import boto3
import os
import logging
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from boto3.dynamodb.conditions import Key
import uuid

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
sqs = boto3.client('sqs')

STATE_TABLE = dynamodb.Table(os.environ['STATE_TABLE_NAME'])
DAILY_CAP_TABLE = dynamodb.Table(os.environ['DAILY_CAP_TABLE_NAME'])
CONFIG_TABLE = dynamodb.Table(os.environ['CONFIG_TABLE_NAME'])
PURCHASE_TABLE = dynamodb.Table(os.environ['PURCHASE_TABLE_NAME'])
FOLLOWUP_QUEUE_URL = os.environ['FOLLOWUP_QUEUE_URL']

JOURNEY_ID = 'abandoned_checkout_mvp'


def lambda_handler(event, context):
    config = _get_journey_config()
    if not config or not config.get('isActive'):
        logger.info("Journey inactive — skipping all records")
        return

    for record in event['Records']:
        try:
            body = json.loads(record['body'])
            _process_user_cart(body, config)
        except Exception as e:
            logger.error(f"Failed to process record: {e}", exc_info=True)
            # Re-raise so SQS retries this message
            raise


def _process_user_cart(message, config):
    user_id = message['userId']
    cart_id = message['cartId']
    events = message['events']

    logger.info(f"Processing {len(events)} events for user={user_id} cart={cart_id}")

    # --- Step 1: Load current state fresh from DynamoDB ---
    state = _get_state(user_id, cart_id)

    # --- Step 2: Apply all events to state in memory ---
    state = _apply_events(state, events, user_id, cart_id)

    # --- Step 3: Run eligibility and suppression checks ---
    if state['cartStatus'] == 'purchased':
        # A2: Purchase completed — cancel any pending follow-up
        state = _cancel_followup(state, 'purchase_completed')
        _save_state(state)
        return

    if state['cartStatus'] == 'emptied':
        # A3: Cart emptied — cancel
        state = _cancel_followup(state, 'cart_emptied')
        _save_state(state)
        return

    # --- Step 4: Decide if this is an abandonment ---
    is_abandoned = _is_abandoned(state, events)

    if not is_abandoned:
        _save_state(state)
        return

    # --- Step 5: Run eligibility checks (Group F, consent) ---
    eligibility = _check_eligibility(user_id, state, config)
    if not eligibility['eligible']:
        state['journeyStatus'] = 'suppressed'
        state['followupCancelReason'] = eligibility['reason']
        _save_state(state)
        logger.info(f"User {user_id} suppressed: {eligibility['reason']}")
        return

    # --- Step 6: Schedule follow-up if not already scheduled ---
    if state['journeyStatus'] not in ('followup_scheduled', 'email_sent', 'email_cancelled'):
        delay_minutes = config['evaluationDelayMinutes']
        send_at = _calculate_send_time(state['cartLastUpdatedAt'], delay_minutes)

        state['journeyStatus'] = 'followup_scheduled'
        state['followupScheduledFor'] = send_at.isoformat()

        _enqueue_followup(user_id, cart_id, state, send_at)
        logger.info(f"Follow-up scheduled for user={user_id} at {send_at.isoformat()}")

    elif state['journeyStatus'] == 'followup_scheduled':
        # A4: Cart was updated — recalculate send time (clock reset)
        delay_minutes = config['evaluationDelayMinutes']
        send_at = _calculate_send_time(state['cartLastUpdatedAt'], delay_minutes)
        state['followupScheduledFor'] = send_at.isoformat()
        _enqueue_followup(user_id, cart_id, state, send_at)
        logger.info(f"Follow-up rescheduled (clock reset) for user={user_id} at {send_at.isoformat()}")

    _save_state(state)


# ---------------------------------------------------------------------------
# Event application
# ---------------------------------------------------------------------------

def _apply_events(state, events, user_id, cart_id):
    """Apply each raw event to the state dict in chronological order."""
    if not state:
        state = _empty_state(user_id, cart_id)

    for e in events:
        event_type = e.get('eventType')
        ts = e.get('eventTimestamp', e.get('receivedAt', ''))

        if event_type in ('add_to_cart', 'remove_from_cart', 'view_cart', 'checkout'):
            state['cartLastUpdatedAt'] = ts
            state['cartValue'] = e.get('cartValue', state.get('cartValue', 0))
            state['cartConsent'] = e.get('cartConsent', state.get('cartConsent', 'unknown'))
            state['dealerCode'] = e.get('dealerCode', state.get('dealerCode', ''))
            state['dealerTDA'] = e.get('dealerTDA', state.get('dealerTDA', ''))
            state['cartType'] = e.get('cartType', state.get('cartType', 'auth'))

            if event_type == 'checkout':
                state['checkoutInitiatedAt'] = ts
                state['cartStatus'] = 'abandoned'  # tentative until purchase

            cart_val = state.get('cartValue', 0)
            if cart_val is not None and float(cart_val) <= 0:
                state['cartStatus'] = 'emptied'
            else:
                if state.get('cartStatus') != 'purchased':
                    state['cartStatus'] = 'active'

        elif event_type == 'purchase':
            state['cartStatus'] = 'purchased'
            state['purchaseCompletedAt'] = ts
            state['cartLastUpdatedAt'] = ts
            # Record purchase for future recent-purchaser checks
            _record_purchase(user_id, cart_id, e)

        elif event_type in ('consent_given', 'consent_revoked'):
            state['cartConsent'] = 'given' if event_type == 'consent_given' else 'revoked'

        state['updatedAt'] = ts

    return state


# ---------------------------------------------------------------------------
# Eligibility checks (Groups A–F)
# ---------------------------------------------------------------------------

def _check_eligibility(user_id, state, config):
    """Run all suppression checks. Returns {eligible: bool, reason: str}."""

    # Consent check
    consent = state.get('cartConsent', 'unknown')
    if consent != 'given':
        return {'eligible': False, 'reason': f'consent_{consent}'}

    # Cart value threshold (checked at evaluation time — Scenario C2)
    cart_value = float(state.get('cartValue', 0))
    threshold = float(config.get('cartValueThreshold', 25))
    if cart_value < threshold:
        return {'eligible': False, 'reason': f'cart_value_below_threshold_{cart_value}'}

    # Excluded dealer check
    excluded = config.get('excludedDealerCodes', [])
    if state.get('dealerCode') in excluded:
        return {'eligible': False, 'reason': f"dealer_excluded_{state.get('dealerCode')}"}

    # Recent purchaser check (Group F)
    lookback_days = int(config.get('recentPurchaserLookbackDays', 30))
    if _has_recent_purchase(user_id, lookback_days):
        return {'eligible': False, 'reason': f'recent_purchaser_within_{lookback_days}_days'}

    return {'eligible': True, 'reason': None}


def _is_abandoned(state, events):
    """
    A cart is considered abandoned if:
    - A checkout event was seen, AND
    - No purchase event in the current batch
    """
    event_types = {e.get('eventType') for e in events}
    has_checkout = 'checkout' in event_types or state.get('checkoutInitiatedAt')
    has_purchase = 'purchase' in event_types or state.get('cartStatus') == 'purchased'
    return has_checkout and not has_purchase

def _calculate_send_time(cart_last_updated_at, delay_minutes):
    """Send time = cartLastUpdatedAt + delay (Scenario A4 clock reset logic)."""
    last_update = datetime.fromisoformat(cart_last_updated_at.replace('Z', '+00:00'))
    return last_update + timedelta(minutes=int(delay_minutes))

#def _calculate_send_time(cart_last_updated_at, delay_minutes):
#   """Send time = cartLastUpdatedAt + delay (Scenario A4 clock reset logic)."""
#  last_update = datetime.fromisoformat(cart_last_updated_at.replace('Z', '+00:00'))
# return last_update + timedelta(minutes=delay_minutes)


# ---------------------------------------------------------------------------
# Follow-up queue
# ---------------------------------------------------------------------------

def _enqueue_followup(user_id, cart_id, state, send_at):
    message = {
        'userId': user_id,
        'cartId': cart_id,
        'dealerCode': state.get('dealerCode'),
        'dealerTDA': state.get('dealerTDA'),
        'cartValue': state.get('cartValue'),
        'cartType': state.get('cartType'),
        'sendAt': send_at.isoformat(),
        'scheduledAt': datetime.now(timezone.utc).isoformat(),
        'journeyId': JOURNEY_ID
    }
    sqs.send_message(
        QueueUrl=FOLLOWUP_QUEUE_URL,
        MessageBody=json.dumps(message),
        MessageGroupId=user_id,
        MessageDeduplicationId=f"{user_id}#{cart_id}#{send_at.isoformat()}"
    )


def _cancel_followup(state, reason):
    if state.get('journeyStatus') == 'followup_scheduled':
        state['journeyStatus'] = 'email_cancelled'
        state['followupCancelledAt'] = datetime.now(timezone.utc).isoformat()
        state['followupCancelReason'] = reason
        logger.info(f"Follow-up cancelled: {reason} for cart {state.get('cartId')}")
    return state


# ---------------------------------------------------------------------------
# DynamoDB helpers
# ---------------------------------------------------------------------------

def _floats_to_decimal(obj):
    """Recursively convert all float values to Decimal for DynamoDB compatibility."""
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: _floats_to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_floats_to_decimal(i) for i in obj]
    return obj

def _get_state(user_id, cart_id):
    resp = STATE_TABLE.get_item(Key={'userId': user_id, 'cartId': cart_id})
    return resp.get('Item')

def _save_state(state):
    state['updatedAt'] = datetime.now(timezone.utc).isoformat()
    STATE_TABLE.put_item(Item=_floats_to_decimal(state))

#def _save_state(state):
#    state['updatedAt'] = datetime.now(timezone.utc).isoformat()
#   STATE_TABLE.put_item(Item=state)


def _empty_state(user_id, cart_id):
    now = datetime.now(timezone.utc).isoformat()
    return {
        'userId': user_id,
        'cartId': cart_id,
        'cartStatus': 'active',
        'journeyStatus': 'pending_evaluation',
        'cartValue': 0,
        'cartConsent': 'unknown',
        'dealerCode': None,
        'dealerTDA': None,
        'cartType': None,
        'cartLastUpdatedAt': now,
        'checkoutInitiatedAt': None,
        'purchaseCompletedAt': None,
        'followupScheduledFor': None,
        'followupSentAt': None,
        'followupCancelledAt': None,
        'followupCancelReason': None,
        'lastEmailSentDate': None,
        'createdAt': now,
        'updatedAt': now
    }


def _get_journey_config():
    resp = CONFIG_TABLE.get_item(Key={'journeyId': JOURNEY_ID})
    return resp.get('Item')


def _has_recent_purchase(user_id, lookback_days):
    """Query UserPurchaseHistory for any purchase within the lookback window."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).isoformat()
    resp = PURCHASE_TABLE.query(
        KeyConditionExpression=Key('userId').eq(user_id) & Key('purchasedAt').gte(cutoff)
    )
    return len(resp.get('Items', [])) > 0


def _record_purchase(user_id, cart_id, event):
    """Write to UserPurchaseHistory when a purchase event is seen."""
    now = datetime.now(timezone.utc)
    ttl = int((now + timedelta(days=35)).timestamp())
    PURCHASE_TABLE.put_item(Item={
        'userId': user_id,
        'purchasedAt': event.get('eventTimestamp', now.isoformat()),
        'cartId': cart_id,
        'dealerCode': event.get('dealerCode', ''),
        'orderValue': event.get('cartValue', 0),
        'ttlExpiry': ttl
    })