{{ config(
    materialized='view',
    tags=['shopify', 'staging', 'products']
) }}

with source as (
    select * from {{ source('shopify_raw', 'shopify_products') }}
),

renamed as (
    select
        -- IDs
        id::varchar as product_id,
        'shopify' as platform,
        
        -- Product details
        title as product_name,
        handle as product_slug,
        product_type as category,
        vendor as brand,
        
        -- Pricing (from first variant)
        (variants->0->>'price')::decimal(12,2) as price,
        (variants->0->>'compare_at_price')::decimal(12,2) as compare_at_price,
        (variants->0->>'sku')::varchar as sku,
        
        -- Inventory
        (variants->0->>'inventory_quantity')::int as inventory_quantity,
        
        -- Status
        status as product_status,
        published_at::timestamp as published_at,
        
        -- Descriptions
        body_html as description,
        tags as product_tags,
        
        -- Images
        (images->0->>'src')::varchar as primary_image_url,
        
        -- Timestamps
        created_at::timestamp as created_at,
        updated_at::timestamp as updated_at,
        
        -- Metadata
        current_timestamp as _loaded_at
        
    from source
)

select * from renamed
