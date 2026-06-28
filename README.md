# pco-abandoned-cart
MVP of PCO Market Basket OneId

# PCO Abandoned Cart — Event Processing Pipeline

AWS serverless pipeline for Toyota Parts Center Online abandoned cart remarketing.

## Architecture
- **API Gateway** — receives browser events via POST /events
- **EventIntake Lambda** — validates and timestamps events
- **EventAggregator Lambda** — batches events by userId (1-min window)
- **JourneyRouter Lambda** — applies BRD rules, schedules follow-ups
- **SendMessage Lambda** — send-time checks and SES email delivery

## Infrastructure
All AWS resources are defined in `infrastructure/pco_abandoned_cart_stack.yaml`.
Deploy with CloudFormation — see deployment steps below.

## Deployment
1. Upload `infrastructure/pco_abandoned_cart_stack.yaml` to CloudFormation
2. Set parameters (Environment, FromEmail, etc.)
3. After stack creation, paste full code for JourneyRouter and SendMessage
4. Seed JourneyConfig table with business rules
5. Verify SES sender email

## Lambda functions
| Function | Purpose |
|---|---|
| event_intake | Validates payload, stamps server-side timestamp |
| event_aggregator | Groups events by userId+cartId, emits FIFO message |
| journey_router | Decision engine — applies Groups A-F BRD rules |
| send_message | Send-time checks and SES email delivery |

## Environment variables
See CloudFormation template for all required environment variables per function.
