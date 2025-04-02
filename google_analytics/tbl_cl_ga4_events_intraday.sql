{{ config(
    tags=['google_analytics_hourly','marketing']
) }}

with
    shp_data as (
        select
            shopify_order_id,
            sum(quantity) as quantity,
            count(distinct case when gift_card_flag='OTHERS' then sku when gift_card_flag='GIFT CARD' then lower(title) else 'NA' end) as unique_items,
            sum(gross_sales) as gross_sales,
            sum(gsnd) as gsnd,
            sum(gross_merchandise_value) as gmv,
            sum(shipping_price) as shipping,
            sum(total_tax+vat_value) as total_tax
        from {{ ref("fact_order_refund") }} shp
        where
            exists (
                select distinct ecommerce_transaction_id
                from {{ source("ga4", "analytics_intraday_344079407__view") }} ga
                where
                    trim(ga.ecommerce_transaction_id) = shp.shopify_order_id and trim(event_name) = 'purchase'
                    and ga.event_date > (select coalesce(max(event_date), '1900-01-01') from {{ ref('tbl_cl_ga4_events') }})
            )
            and sale_kind = 'ORDER'
            and country = 'US'
        group by shopify_order_id
    ),

    ga_data as (
        select
            event_date,
            to_timestamp(
                convert_timezone('America/New_York', to_timestamp_tz(event_timestamp))
            ) as event_timestamp,
            trim(event_name) event_name,
            event_bundle_sequence_id,
            trim(user_id) as user_id,
            trim(user_pseudo_id::string) as user_pseudo_id,
            is_active_user,
            to_timestamp(
                convert_timezone(
                    'America/New_York', to_timestamp_tz(user_first_touch_timestamp)
                )
            ) as user_first_touch_timestamp,
            lower(trim(traffic_source_source)) as traffic_source_source,
            lower(trim(traffic_source_medium)) as traffic_source_medium,
            lower(trim(traffic_source_name)) as traffic_source_name,
            lower(
                trim(collected_traffic_source_manual_source)
            ) as collected_traffic_source_manual_source,
            lower(
                trim(collected_traffic_source_manual_medium)
            ) as collected_traffic_source_manual_medium,
            trim(
                collected_traffic_source_manual_campaign_id
            ) as collected_traffic_source_manual_campaign_id,
            lower(
                trim(collected_traffic_source_manual_campaign_name)
            ) as collected_traffic_source_manual_campaign_name,
            lower(
                trim(collected_traffic_source_manual_source_platform)
            ) as collected_traffic_source_manual_source_platform,
            lower(
                trim(collected_traffic_source_manual_term)
            ) as collected_traffic_source_manual_term,
            lower(
                trim(collected_traffic_source_manual_content)
            ) as collected_traffic_source_manual_content,
            lower(
                trim(session_traffic_source_last_click_manual_campaign_source)
            ) as session_traffic_source_last_click_manual_campaign_source,
            lower(
                trim(session_traffic_source_last_click_manual_campaign_medium)
            ) as session_traffic_source_last_click_manual_campaign_medium,
            trim(
                session_traffic_source_last_click_manual_campaign_campaign_id
            ) as session_traffic_source_last_click_manual_campaign_campaign_id,
            lower(
                trim(session_traffic_source_last_click_manual_campaign_campaign_name)
            ) as session_traffic_source_last_click_manual_campaign_campaign_name,
            lower(
                trim(session_traffic_source_last_click_manual_campaign_source_platform)
            ) as session_traffic_source_last_click_manual_campaign_source_platform,
            lower(
                trim(session_traffic_source_last_click_manual_campaign_term)
            ) as session_traffic_source_last_click_manual_campaign_term,
            lower(
                trim(session_traffic_source_last_click_manual_campaign_content)
            ) as session_traffic_source_last_click_manual_campaign_content,
            case
                when trim(ecommerce_transaction_id) = '(not set)'
                then null
                else trim(ecommerce_transaction_id)
            end as shopify_order_id,
            coalesce(shp.unique_items,ecommerce_unique_items) unique_items,
            coalesce(shp.quantity,ecommerce_total_item_quantity) total_item_quantity,
            ecommerce_purchase_revenue_in_usd,
            shp.total_tax tax_value,
            shp.shipping shipping_value,
            shp.gross_sales,
            shp.gsnd,
            shp.gmv,
            event_params,
            row_number() over (order by event_timestamp) as s_no
        from {{ source("ga4", "analytics_intraday_344079407__view") }} ga
        left join
            shp_data shp on trim(ga.ecommerce_transaction_id) = shp.shopify_order_id
        where event_date 
            > (select coalesce(max(event_date), '1900-01-01') from {{ ref('tbl_cl_ga4_events') }})
    ),

    event_params as (
        select
            a.*,
            ep.value:key::string event_key,
            coalesce(
                ep.value:value:string_value::string,
                ep.value:value:int_value::string,
                ep.value:value:double_value::string,
                ep.value:value:float_value::string
            ) as event_value,
        from ga_data a, lateral flatten(input => event_params) as ep
    ),

    res as (
        select
            event_date,
            event_timestamp,
            hour(event_timestamp) as hourofday,
            s_no,
            event_name,
            event_bundle_sequence_id,
            user_id,
            user_pseudo_id,
            coalesce(user_pseudo_id, user_id) unique_user_id,
            is_active_user,
            user_first_touch_timestamp,
            trim("'ga_session_id'") as ga_session_id,
            coalesce(
                "'ga_session_id'" || ':' || user_pseudo_id,
                unique_user_id,
                "'ga_session_id'"
            ) as unique_session_id,
            {{ udf_cl_ga_source("traffic_source_source") }} traffic_source_source,
            {{ udf_cl_ga_medium("traffic_source_medium") }} traffic_source_medium,
            traffic_source_name,
            {{ udf_cl_ga_source("collected_traffic_source_manual_source") }} collected_traffic_source_manual_source,
            {{ udf_cl_ga_medium("collected_traffic_source_manual_medium") }} collected_traffic_source_manual_medium,
            collected_traffic_source_manual_campaign_id,
            collected_traffic_source_manual_campaign_name,
            collected_traffic_source_manual_source_platform,
            collected_traffic_source_manual_term,
            collected_traffic_source_manual_content,
            {{
                udf_cl_ga_source(
                    "session_traffic_source_last_click_manual_campaign_source"
                )
            }} session_traffic_source_last_click_manual_campaign_source,
            {{
                udf_cl_ga_medium(
                    "session_traffic_source_last_click_manual_campaign_medium"
                )
            }} session_traffic_source_last_click_manual_campaign_medium,
            session_traffic_source_last_click_manual_campaign_campaign_id,
            session_traffic_source_last_click_manual_campaign_campaign_name,
            session_traffic_source_last_click_manual_campaign_source_platform,
            session_traffic_source_last_click_manual_campaign_term,
            session_traffic_source_last_click_manual_campaign_content,
            trim("'ga_session_number'") as ga_session_number,
            trim("'page_location'") as page_location,
            trim("'link_url'") as link_url,
            trim("'link_domain'") as link_domain,
            trim("'page_title'") as page_title,
            trim("'page_referrer'") as page_referrer,
            -- trim("'campaign_id'") as campaign_id,
            -- trim("'campaign'") as campaign,
            -- trim("'content'") as content,
            -- trim("'term'") as term,
            trim("'batch_page_id'") as batch_page_id,
            trim("'batch_ordering_id'") as batch_ordering_id,
            trim("'exp_variant_string'") as exp_variant_string,
            trim("'engagement_time_msec'") as engagement_time_msec,
            trim("'category'") as category,
            trim("'action'") as action,
            --trim("'item_list_id'") as item_list_id,
            trim("'percent_scrolled'") as percent_scrolled,
            trim("'visitor_type'") as visitor_type,
            trim("'search_term'") as search_term,
            trim("'label'") as label,
            trim("'currency'") as currency,
            shopify_order_id,
            unique_items,
            total_item_quantity,
            ecommerce_purchase_revenue_in_usd,
            tax_value,
            shipping_value,
            gross_sales,
            gsnd,
            gmv,
        from
            event_params pivot (
                max(event_value) for event_key in (
                    'ga_session_id',
                    'ga_session_number',
                    'page_location',
                    'link_url',
                    'link_domain',
                    'page_title',
                    'page_referrer',
                    -- 'campaign_id',
                    -- 'campaign',
                    -- 'content',
                    -- 'term',
                    'batch_page_id',
                    'batch_ordering_id',
                    'exp_variant_string',
                    'engagement_time_msec',
                    'category',
                    'action',
                    --'item_list_id',
                    'percent_scrolled',
                    'visitor_type',
                    'search_term',
                    'label',
                    'currency'
                )
            )
    )
select
    *,
    {{
        udf_acquisition_channel_grouping(
            "traffic_source_source", "traffic_source_medium", "traffic_source_name"
        )
    }} as acquisition_channelgroup_nobull24,
    {{
        udf_session_channel_grouping(
            "collected_traffic_source_manual_source",
            "collected_traffic_source_manual_medium",
            "collected_traffic_source_manual_campaign_name",
            "collected_traffic_source_manual_source_platform"
        )
    }}
    as session_channelgroup_nobull24
from res
