{{ config(
    materialized='view',
    tags=['shopify', 'staging', 'order_items']
) }}

with source as (
    select * from {{ source('shopify_raw', 'shopify_order_line_items') }}
),

renamed as (
    select
        -- IDs
        id::varchar as line_item_id,
        order_id::varchar as order_id,
        product_id::varchar as product_id,
        variant_id::varchar as variant_id,
        'shopify' as platform,
        
        -- Product info
        title as product_name,
        variant_title,
        sku,
        
        -- Quantities & pricing
        quantity::int as quantity,
        price::decimal(12,2) as unit_price,
        (quantity * price)::decimal(12,2) as line_total,
        
        -- Discounts
        coalesce(total_discount::decimal(12,2), 0) as discount_amount,
        
        -- Fulfillment
        fulfillment_status,
        fulfillable_quantity::int as fulfillable_quantity,
        
        -- Flags
        gift_card as is_gift_card,
        taxable as is_taxable,
        requires_shipping as requires_shipping,
        
        -- Metadata
        current_timestamp as _loaded_at
        
    from source
)

select * from renamed
