{{ config(
    materialized='view',
    tags=['lazada', 'staging', 'order_items']
) }}

with source as (
    select * from {{ source('lazada_raw', 'lazada_order_items') }}
),

renamed as (
    select
        -- IDs
        order_item_id::varchar as line_item_id,
        order_id::varchar as order_id,
        product_id::varchar as product_id,
        null::varchar as variant_id,
        'lazada' as platform,
        
        -- Product info
        name as product_name,
        variation as variant_title,
        sku,
        
        -- Quantities & pricing
        1::int as quantity,
        paid_price::decimal(12,2) as unit_price,
        paid_price::decimal(12,2) as line_total,
        
        -- Discounts
        coalesce(voucher_amount::decimal(12,2), 0) as discount_amount,
        
        -- Fulfillment
        status as fulfillment_status,
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
