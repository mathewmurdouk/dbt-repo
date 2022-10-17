select total_amount_paid
from {{ ref('stg_stripe_payments') }}
where total_amount_paid>60