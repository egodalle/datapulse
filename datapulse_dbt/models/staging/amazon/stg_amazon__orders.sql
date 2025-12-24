{{ config(
    materialized='view',
    tags=['amazon', 'staging', 'orders']
) }}

with source as (
    select * from {{ source('amazon_raw', 'amazon_orders') }}
),

renamed as (
    select
        -- IDs
        amazon_order_id::varchar as order_id,
        'amazon' as platform,
        
        -- Timestamps
        purchase_date::timestamp as order_created_at,
        last_update_date::timestamp as order_updated_at,
        null::timestamp as order_processed_at,
        null::timestamp as order_cancelled_at,
        null::timestamp as order_closed_at,
        
        -- Customer info
        buyer_email as customer_id,
        buyer_email as customer_email,
        
        -- Financials
        (order_total->>'Amount')::decimal(12,2) as total_amount,
        (order_total->>'Amount')::decimal(12,2) as subtotal_amount,
        0::decimal(12,2) as tax_amount,
        0::decimal(12,2) as discount_amount,
        (order_total->>'CurrencyCode')::varchar as currency_code,
        
        -- Status
        payment_method as payment_status,
        order_status as fulfillment_status,
        null as cancel_reason,
        
        -- Counts
        number_of_items_shipped + number_of_items_unshipped as item_count,
        
        -- Additional info
        sales_channel as order_source,
        null as order_tags,
        null as order_notes,
        
        -- Metadata
        current_timestamp as _loaded_at
        
    from source
)

select * from renamed
