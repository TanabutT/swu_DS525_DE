select
    order_status
from {{ ref('completed_orders') }}
where order_status != 'completed'