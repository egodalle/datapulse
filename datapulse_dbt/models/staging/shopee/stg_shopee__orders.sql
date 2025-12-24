{{ config(
    materialized='view',
    tags=['shopee', 'staging', 'orders']
) }}

with source as (
    select * from {{ source('shopee_raw', 'shopee_orders') }}
),

renamed as (
    select
        -- IDs
        order_sn::varchar as order_id,
        'shopee' as platform,
        
        -- Timestamps
        to_timestamp(create_time) as order_created_at,
        to_timestamp(update_time) as order_updated_at,
        to_timestamp(pay_time) as order_processed_at,
        case when order_status = 'CANCELLED' then to_timestamp(update_time) else null end as order_cancelled_at,
        case when order_status = 'COMPLETED' then to_timestamp(update_time) else null end as order_closed_at,
        
        -- Customer info
        buyer_user_id::varchar as customer_id,
        buyer_username as customer_email,
        
        -- Financials
        total_amount::decimal(12,2) as total_amount,
        (total_amount - coalesce(estimated_shipping_fee, 0))::decimal(12,2) as subtotal_amount,
        0::decimal(12,2) as tax_amount,
        coalesce(voucher_absorbed::decimal(12,2), 0) as discount_amount,
        currency as currency_code,
        
        -- Status
        case 
            when order_status in ('READY_TO_SHIP', 'PROCESSED', 'SHIPPED', 'COMPLETED') then 'paid'
            when order_status = 'UNPAID' then 'pending'
            else 'unknown'
        end as payment_status,
        order_status as fulfillment_status,
        cancel_reason,
        
        -- Counts
        1 as item_count,
        
        -- Additional info
        'shopee' as order_source,
        null as order_tags,
        message_to_seller as order_notes,
        
        -- Metadata
        current_timestamp as _loaded_at
        
    from source
)

select * from renamed
