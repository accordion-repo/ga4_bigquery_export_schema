{{
    config(
        materialized="incremental",
        unique_key="event_date",
        transient=false,
        tags=['marketing'],
        on_schema_change="sync_all_columns",
        pre_hook="
        {%- set target_relation=adapter.get_relation(
                database=this.database,
                schema=this.schema,
                identifier=this.name) -%}       
        
        {%- set table_exists=target_relation is not none -%}
        {%- if table_exists -%}
        delete from {{this}} where event_date = (select max(event_date) from {{this}})
        {%- endif -%}"
    )
}}

with
    shp_data as (
        select
            shopify_order_id,
            case
                when gift_card_flag = 'GIFT CARD' then lower(title) else sku
            end as sku,
            sum(quantity) as quantity,
            sum(gross_sales) as gross_sales,
            sum(gsnd) as gsnd,
            sum(gross_merchandise_value) as gmv,
            sum(shipping_price) as shipping,
            sum(total_tax + vat_value) as total_tax
        from {{ ref("fact_order_refund") }} shp
        where
            exists (
                select distinct ecommerce_transaction_id
                from {{ source("ga4", "analytics_344079407__view") }} ga
                where
                    trim(ga.ecommerce_transaction_id) = shp.shopify_order_id
                    and ga.event_date
                    > (select coalesce(max(event_date), '1900-01-01') from {{ this }})
            )
            and sale_kind = 'ORDER'
            and country = 'US'
        group by 1, 2
    ),

    ga_data_initial as (
        select *, row_number() over (order by event_timestamp) as s_no
        from {{ source("ga4", "analytics_344079407__view") }}
        where
            event_date
            > (select coalesce(max(event_date), '1900-01-01') from {{ this }})
    ),
    ga_data as (
        select
            event_date,
            to_timestamp(
                convert_timezone('America/New_York', to_timestamp_tz(event_timestamp))
            ) as event_timestamp,
            s_no,
            trim(event_name) event_name,
            trim(user_id) as user_id,
            trim(user_pseudo_id::string) as user_pseudo_id,
            is_active_user,
            case
                when trim(ecommerce_transaction_id) = '(not set)'
                then null
                else trim(ecommerce_transaction_id)
            end as shopify_order_id,
            event_params,
            items
        from ga_data_initial
        where items <> '[]'
    ),
    event_params as (
        select
            a.*,
            ep.value:key::string event_key,
            trim(
                coalesce(
                    ep.value:value:string_value::string,
                    ep.value:value:int_value::string,
                    ep.value:value:double_value::string,
                    ep.value:value:float_value::string
                )
            ) as event_value,
        from ga_data a, lateral flatten(input => event_params) as ep
    ),
    event_res as (
        select
            event_date,
            event_timestamp,
            hour(event_timestamp) as hourofday,
            s_no,
            event_name,
            user_id,
            user_pseudo_id,
            coalesce(user_pseudo_id, user_id) unique_user_id,
            is_active_user,
            trim("'ga_session_id'") as ga_session_id,
            coalesce(
                ga_session_id || ':' || user_pseudo_id, unique_user_id, ga_session_id
            ) as unique_session_id,
            trim("'ga_session_number'") as ga_session_number,
            shopify_order_id,
            trim("'page_location'") as page_location,
            trim("'coupon'") as coupon,
            items
        from
            event_params pivot (
                max(event_value) for event_key
                in ('ga_session_id', 'ga_session_number', 'coupon', 'page_location')
            )
    ),

    items as (
        select
            a.*,
            i.value:item_brand::string as item_brand,
            i.value:item_category::string as item_category,
            i.value:item_id::string as item_id,
            i.value:item_list_name::string as item_list_name,
            i.value:item_name::string as item_name,
            i.value:quantity::number as quantity,
            i.value:price_in_usd::float as price_in_usd,
            i.value:item_params as item_params,
            i.value:item_variant::string as item_variant,
            i.value:item_revenue_in_usd::float as item_revenue_in_usd,
        from event_res a, lateral flatten(input => items) as i
    ),

    item_params as (
        select
            a.*,
            i.value:key::string as item_param_key,
            trim(
                coalesce(
                    i.value:value:string_value::string,
                    i.value:value:int_value::string,
                    i.value:value:double_value::string,
                    i.value:value:float_value::string
                )
            ) as item_param_value,
        from items a, lateral flatten(input => item_params) as i
    ),
    item_params_pur as (
        select
            *,
            case
                when lower(item_name) like '%gift card%'
                then lower(trim(item_name))
                else trim(item_id)
            end as item_id_cleaned,
            sum(item_revenue_in_usd) over (
                partition by shopify_order_id, item_id_cleaned
            )
            / 2 as item_revenue_in_usd_agg,
            row_number() over (
                partition by shopify_order_id, item_id_cleaned
                order by price_in_usd desc
            ) as rnk
        from item_params
        where event_name = 'purchase'
        qualify rnk = 1
    ),
    final as (
        select
            event_date,
            event_timestamp,
            hourofday,
            s_no,
            event_name,
            user_id,
            user_pseudo_id,
            unique_user_id,
            is_active_user,
            ga_session_id,
            unique_session_id,
            ga_session_number,
            item_params.shopify_order_id,
            page_location,
            coupon,
            item_brand,
            trim(item_params.item_id) as item_id,
            p.division,
            p.product_category,
            p.product_subcategory,
            p.product_family,
            p.planning_parent,
            case
                when lower(p.externalid) like '%nvd%'
                then 'SHIPPING PROTECTION SKU'
                when lower(item_name) like '%gift%'
                then 'UNISEX'
                when
                    lower(item_name) like '%men%'
                    and lower(item_name) not like '%women%'
                then 'MALE'
                when lower(item_name) like '%women%'
                then 'FEMALE'
                else p.gender
            end as gender,
            p.strategic_business_unit,
            case
                when trim(item_list_name) in ('', '(not set)')
                then null
                else trim(item_list_name)
            end as item_list_name,
            trim(item_name) as item_name,
            case
                when trim(item_variant) in ('', '(not set)')
                then null
                else trim(item_variant)
            end as item_variant,
            item_params.quantity as quantity,
            item_params.price_in_usd,
            item_params.item_revenue_in_usd,
            null as total_tax,
            null as shipping,
            null as gross_sales,
            null as gsnd,
            null as gmv,
            trim("'item_product_id'") as item_product_id,
            trim("'item_variant_id'") as item_variant_id,
            case
                when contains(trim(item_variant), ' / ') > 0
                then
                    case
                        when
                            array_size(split(trim(item_variant), ' / ')) > 2
                            and p.product_family not ilike '%bras%'
                        then replace(split(trim(item_variant), ' / ')[1], '"', '')
                        else replace(split(trim(item_variant), ' / ')[0], '"', '')
                    end
                else null
            end as color,
            case
                when contains(trim(item_variant), ' / ') > 0
                then
                    case
                        when
                            array_size(split(trim(item_variant), ' / ')) > 2
                            and p.product_family not ilike '%bras%'
                        then replace(split(trim(item_variant), ' / ')[2], '"', '')
                        else replace(split(trim(item_variant), ' / ')[1], '"', '')
                    end
                else null
            end as size
        from
            item_params pivot (
                max(item_param_value) for item_param_key
                in ('item_product_id', 'item_variant_id')
            )
        left join
            {{ ref("dim_product_v2") }} p on p.externalid = trim(item_params.item_id)
        where event_name not in ('view_item_list', 'purchase')

        union all

        select
            event_date,
            event_timestamp,
            hourofday,
            s_no,
            event_name,
            user_id,
            user_pseudo_id,
            unique_user_id,
            is_active_user,
            ga_session_id,
            unique_session_id,
            ga_session_number,
            item_params_pur.shopify_order_id,
            page_location,
            coupon,
            item_brand,
            trim(item_params_pur.item_id) as item_id,
            p.division,
            p.product_category,
            p.product_subcategory,
            p.product_family,
            p.planning_parent,
            case
                when lower(p.externalid) like '%nvd%'
                then 'SHIPPING PROTECTION SKU'
                when lower(item_name) like '%gift%'
                then 'UNISEX'
                when
                    lower(item_name) like '%men%'
                    and lower(item_name) not like '%women%'
                then 'MALE'
                when lower(item_name) like '%women%'
                then 'FEMALE'
                else p.gender
            end as gender,
            p.strategic_business_unit,
            case
                when trim(item_list_name) in ('', '(not set)')
                then null
                else trim(item_list_name)
            end as item_list_name,
            trim(item_name) as item_name,
            case
                when trim(item_variant) in ('', '(not set)')
                then null
                else trim(item_variant)
            end as item_variant,
            coalesce(shp.quantity, item_params_pur.quantity) as quantity,
            item_params_pur.price_in_usd,
            item_params_pur.item_revenue_in_usd_agg as item_revenue_in_usd_agg,
            shp.total_tax,
            shp.shipping,
            shp.gross_sales,
            shp.gsnd,
            shp.gmv,
            trim("'item_product_id'") as item_product_id,
            trim("'item_variant_id'") as item_variant_id,
            case
                when contains(trim(item_variant), ' / ') > 0
                then
                    case
                        when
                            array_size(split(trim(item_variant), ' / ')) > 2
                            and p.product_family not ilike '%bras%'
                        then replace(split(trim(item_variant), ' / ')[1], '"', '')
                        else replace(split(trim(item_variant), ' / ')[0], '"', '')
                    end
                else null
            end as color,
            case
                when contains(trim(item_variant), ' / ') > 0
                then
                    case
                        when
                            array_size(split(trim(item_variant), ' / ')) > 2
                            and p.product_family not ilike '%bras%'
                        then replace(split(trim(item_variant), ' / ')[2], '"', '')
                        else replace(split(trim(item_variant), ' / ')[1], '"', '')
                    end
                else null
            end as size
        from
            item_params_pur pivot (
                max(item_param_value) for item_param_key
                in ('item_product_id', 'item_variant_id')
            )
        left join
            shp_data shp
            on item_params_pur.shopify_order_id = shp.shopify_order_id
            and item_params_pur.item_id_cleaned = shp.sku
            and event_name = 'purchase'
        left join
            {{ ref("dim_product_v2") }} p
            on p.externalid = trim(item_params_pur.item_id)
        where event_name = 'purchase'

        union all

        select
            event_date,
            min(event_timestamp) as event_timestamp,
            hourofday,
            min(s_no) as s_no,
            event_name,
            max(user_id) as user_id,
            max(user_pseudo_id) as user_pseudo_id,
            unique_user_id,
            min(is_active_user) as is_active_user,
            max(ga_session_id) as ga_session_id,
            unique_session_id,
            min(ga_session_number) as ga_session_number,
            null as shopify_order_id,
            max(page_location) as page_location,
            null as coupon,
            'NOBULL' as item_brand,
            null as item_id,
            p.division,
            p.product_category,
            p.product_subcategory,
            p.product_family,
            p.planning_parent,
            case
                when lower(p.externalid) like '%nvd%'
                then 'SHIPPING PROTECTION SKU'
                when lower(item_name) like '%gift%'
                then 'UNISEX'
                when
                    lower(item_name) like '%men%'
                    and lower(item_name) not like '%women%'
                then 'MALE'
                when lower(item_name) like '%women%'
                then 'FEMALE'
                else p.gender
            end as gender,
            p.strategic_business_unit,
            trim(item_list_name) as item_list_name,
            null as item_name,
            null as item_variant,
            null as quantity,
            null as price_in_usd,
            null as item_revenue_in_usd,
            null as tax_value,
            null as shipping,
            null as gross_sales,
            null as gsnd,
            null as gmv,
            null as item_product_id,
            null as item_variant_id,
            null as color,
            null as size
        from items i
        left join {{ ref("dim_product_v2") }} p on p.externalid = trim(i.item_id)
        where
            event_name = 'view_item_list'
            and item_list_name not in ('', '(not set)')
            and item_list_name is not null
        group by 1, 3, 5, 8, 11, 18, 19, 20, 21, 22, 23, 24, 25
    )
select
    event_date,
    event_timestamp,
    hourofday,
    s_no,
    event_name,
    user_id,
    user_pseudo_id,
    unique_user_id,
    is_active_user,
    ga_session_id,
    unique_session_id,
    ga_session_number,
    shopify_order_id,
    page_location,
    coupon,
    item_brand,
    item_id,
    division,
    product_category,
    product_subcategory,
    product_family,
    planning_parent,
    gender,
    strategic_business_unit,
    item_list_name,
    item_name,
    item_variant,
    quantity,
    price_in_usd,
    item_revenue_in_usd,
    total_tax,
    shipping,
    gross_sales,
    gsnd,
    gmv,
    item_product_id,
    item_variant_id,
    case
        when
            color in (
                select distinct product_size
                from {{ ref("dim_product_v2") }}
                where product_size is not null
            )
        then size
        else color
    end as color,
    case
        when
            color in (
                select distinct product_size
                from {{ ref("dim_product_v2") }}
                where product_size is not null
            )
        then color
        else size
    end as size
from final
