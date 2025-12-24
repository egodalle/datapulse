{% macro convert_to_usd(amount, currency_code) %}
    case {{ currency_code }}
        when 'USD' then {{ amount }}
        when 'PHP' then {{ amount }} * {{ var('currency_rates')['PHP'] }}
        when 'MYR' then {{ amount }} * {{ var('currency_rates')['MYR'] }}
        when 'SGD' then {{ amount }} * {{ var('currency_rates')['SGD'] }}
        when 'IDR' then {{ amount }} * {{ var('currency_rates')['IDR'] }}
        else {{ amount }}
    end
{% endmacro %}

{% macro get_order_status_standardized(platform, status_field) %}
    case '{{ platform }}'
        when 'shopify' then
            case {{ status_field }}
                when 'paid' then 'completed'
                when 'pending' then 'pending'
                when 'refunded' then 'refunded'
                else {{ status_field }}
            end
        when 'amazon' then
            case {{ status_field }}
                when 'Shipped' then 'completed'
                when 'Pending' then 'pending'
                when 'Canceled' then 'cancelled'
                else {{ status_field }}
            end
        when 'lazada' then
            case {{ status_field }}
                when 'delivered' then 'completed'
                when 'pending' then 'pending'
                when 'canceled' then 'cancelled'
                else {{ status_field }}
            end
        when 'shopee' then
            case {{ status_field }}
                when 'COMPLETED' then 'completed'
                when 'READY_TO_SHIP' then 'processing'
                when 'CANCELLED' then 'cancelled'
                else {{ status_field }}
            end
        else {{ status_field }}
    end
{% endmacro %}

