{{ config(
    materialized='view',
    tags=['shopify', 'staging', 'customers']
) }}

with source as (
    select * from {{ source('shopify_raw', 'shopify_customers') }}
),

renamed as (
    select
        -- IDs
        id::varchar as customer_id,
        'shopify' as platform,
        
        -- Contact info
        email as customer_email,
        phone as customer_phone,
        
        -- Name
        first_name,
        last_name,
        concat(first_name, ' ', last_name) as full_name,
        
        -- Location
        (default_address->>'city')::varchar as city,
        (default_address->>'province')::varchar as state_province,
        (default_address->>'country')::varchar as country,
        (default_address->>'country_code')::varchar as country_code,
        
        -- Order metrics
        orders_count::int as total_orders,
        total_spent::decimal(12,2) as total_spent,
        
        -- Status
        state as customer_status,
        verified_email as is_email_verified,
        accepts_marketing as accepts_marketing,
        
        -- Tags
        tags as customer_tags,
        
        -- Timestamps
        created_at::timestamp as created_at,
        updated_at::timestamp as updated_at,
        
        -- Metadata
        current_timestamp as _loaded_at
        
    from source
)

select * from renamed
