{{ config(
    materialized='view',
    tags=['lazada', 'staging', 'orders']
) }}

with source as (
    select * from {{ source('lazada_raw', 'lazada_orders') }}
),

renamed as (
    select
        -- IDs
        order_id::varchar as order_id,
        'lazada' as platform,
        
        -- Timestamps
        created_at::timestamp as order_created_at,
        updated_at::timestamp as order_updated_at,
        null::timestamp as order_processed_at,
        null::timestamp as order_cancelled_at,
        null::timestamp as order_closed_at,
        
        -- Customer info
        customer_id::varchar as customer_id,
        buyer_email as customer_email,
        
        -- Financials
        price::decimal(12,2) as total_amount,
        price::decimal(12,2) as subtotal_amount,
        0::decimal(12,2) as tax_amount,
        coalesce(voucher::decimal(12,2), 0) as discount_amount,
        'PHP' as currency_code,
        
        -- Status
        payment_method as payment_status,
        statuses as fulfillment_status,
        null as cancel_reason,
        
        -- Counts
        items_count::int as item_count,
        
        -- Additional info
        'lazada' as order_source,
        null as order_tags,
        remarks as order_notes,
        
        -- Metadata
        current_timestamp as _loaded_at
        
    from source
)

select * from renamed
