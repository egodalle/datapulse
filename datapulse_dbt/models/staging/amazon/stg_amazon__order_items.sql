{{ config(
    materialized='view',
    tags=['amazon', 'staging', 'order_items']
) }}

with source as (
    select * from {{ source('amazon_raw', 'amazon_order_items') }}
),

renamed as (
    select
        -- IDs
        order_item_id::varchar as line_item_id,
        amazon_order_id::varchar as order_id,
        asin::varchar as product_id,
        null::varchar as variant_id,
        'amazon' as platform,
        
        -- Product info
        title as product_name,
        null as variant_title,
        seller_sku as sku,
        
        -- Quantities & pricing
        quantity_ordered::int as quantity,
        (item_price->>'Amount')::decimal(12,2) / nullif(quantity_ordered, 0) as unit_price,
        (item_price->>'Amount')::decimal(12,2) as line_total,
        
        -- Discounts
        coalesce((promotion_discount->>'Amount')::decimal(12,2), 0) as discount_amount,
        
        -- Fulfillment
        case 
            when quantity_shipped > 0 then 'shipped'
            else 'pending'
        end as fulfillment_status,
        (quantity_ordered - quantity_shipped)::int as fulfillable_quantity,
        
        -- Flags
        false as is_gift_card,
        true as is_taxable,
        true as requires_shipping,
        
        -- Metadata
        current_timestamp as _loaded_at
        
    from source
)

select * from renamed
