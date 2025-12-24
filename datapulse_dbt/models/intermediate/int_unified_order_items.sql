{{ config(
    materialized='view',
    tags=['intermediate', 'order_items']
) }}

/*
    Unified order items from all e-commerce platforms
*/

with shopify_items as (
    select * from {{ ref('stg_shopify__order_items') }}
),

amazon_items as (
    select * from {{ ref('stg_amazon__order_items') }}
),

lazada_items as (
    select * from {{ ref('stg_lazada__order_items') }}
),

shopee_items as (
    select * from {{ ref('stg_shopee__order_items') }}
),

unified as (
    select * from shopify_items
    union all
    select * from amazon_items
    union all
    select * from lazada_items
    union all
    select * from shopee_items
)

select * from unified

