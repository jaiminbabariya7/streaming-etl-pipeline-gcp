-- Fails if any purchase event has a missing or non-positive amount.
SELECT event_id, purchase_amount_usd
FROM {{ ref('fct_events') }}
WHERE event_type = 'PURCHASE'
  AND (purchase_amount_usd IS NULL OR purchase_amount_usd <= 0)
