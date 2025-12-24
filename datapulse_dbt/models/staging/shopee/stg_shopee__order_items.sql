{{ config(
    materialized='view',
    tags=['shopee', 'staging', 'order_items']
) }}

with source as (
    select * from {{ source('shopee_raw', 'shopee_order_items') }}
),

renamed as (
    select
        -- IDs
        (order_sn || '_' || item_id::varchar)::varchar as line_item_id,
        order_sn::varchar as order_id,
        item_id::varchar as product_id,
        model_id::varchar as variant_id,
        'shopee' as platform,
        
        -- Product info
        item_name as product_name,
        model_name as variant_title,
        model_sku as sku,
        
        -- Quantities & pricing
        model_quantity_purchased::int as quantity,
        model_discounted_price::decimal(12,2) as unit_price,
        (model_quantity_purchased * model_discounted_price)::decimal(12,2) as line_total,
        
        -- Discounts
        coalesce((model_original_price - model_discounted_price) * model_quantity_purchased, 0)::decimal(12,2) as discount_amount,
        
        -- Fulfillment
        'pending' as fulfillment_status,
        0::int as fulfillable_quantity,
        
        -- Flags
        false as is_gift_card,
        true as is_taxable,
        true as requires_shipping,
        
        -- Metadata
        current_timestamp as _loaded_at
        
    from source
)

select * from renamed
