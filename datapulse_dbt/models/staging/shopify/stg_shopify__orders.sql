{{ config(
    materialized='view',
    tags=['shopify', 'staging', 'orders']
) }}

with source as (
    select * from {{ source('shopify_raw', 'shopify_orders') }}
),

renamed as (
    select
        -- IDs
        id::varchar as order_id,
        'shopify' as platform,
        
        -- Timestamps
        created_at::timestamp as order_created_at,
        updated_at::timestamp as order_updated_at,
        processed_at::timestamp as order_processed_at,
        cancelled_at::timestamp as order_cancelled_at,
        closed_at::timestamp as order_closed_at,
        
        -- Customer info
        customer_id::varchar as customer_id,
        email as customer_email,
        
        -- Financials
        total_price::decimal(12,2) as total_amount,
        subtotal_price::decimal(12,2) as subtotal_amount,
        total_tax::decimal(12,2) as tax_amount,
        total_discounts::decimal(12,2) as discount_amount,
        currency as currency_code,
        
        -- Status
        financial_status as payment_status,
        fulfillment_status,
        cancel_reason,
        
        -- Counts
        coalesce(line_items_count, 0) as item_count,
        
        -- Additional info
        source_name as order_source,
        tags as order_tags,
        note as order_notes,
        
        -- Metadata
        current_timestamp as _loaded_at
        
    from source
)

select * from renamed
