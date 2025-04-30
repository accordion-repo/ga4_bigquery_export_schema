{{
    config(
       tags=['google_analytics_hourly','marketing']
    )
}}

with
    first_visit_users as (
        select distinct event_date, unique_user_id
        from {{ ref("tbl_cl_ga4_events_intraday") }}
        where
            event_name = 'first_visit'
            and event_date > (
                select coalesce(max(event_date), '1900-01-01')
                from {{ ref("tbl_cl_ga4_sessions") }}
            )
    ),
    activity_check1 as (
        select t1.*, count(t2.event_name) as activity1
        from first_visit_users t1
        left join
            {{ ref("tbl_cl_ga4_events") }} t2
            on t1.unique_user_id = t2.unique_user_id
            and t2.event_date
            between dateadd(day, -31, t1.event_date) and dateadd(day, -1, t1.event_date)
        group by t1.unique_user_id, t1.event_date
    ),
    activity_check2 as (
        select t1.*, count(t2.event_name) as activity2
        from first_visit_users t1
        left join
            {{ ref("tbl_cl_ga4_events_intraday") }} t2
            on t1.unique_user_id = t2.unique_user_id
            and t2.event_date
            between dateadd(day, -31, t1.event_date) and dateadd(day, -1, t1.event_date)
        group by t1.unique_user_id, t1.event_date
    ),
    purchase_check as (
        select
            ecomm.event_date,
            ecomm.unique_user_id,
            ecomm.shopify_order_id,
            ct.customer_type
        from {{ ref("tbl_cl_ga4_ecommerce_intraday") }} ecomm
        left join
            {{ ref("fact_customer_type") }} ct
            on ecomm.shopify_order_id = ct.shopify_order_id
            and ecomm.event_date > (
                select coalesce(max(event_date), '1900-01-01')
                from {{ ref("tbl_cl_ga4_sessions") }}
            )
    ),
    new_users as (
        select distinct ac1.event_date, ac1.unique_user_id
        from activity_check1 ac1
        left join
            activity_check2 ac2
            on ac1.event_date = ac2.event_date
            and ac1.unique_user_id = ac2.unique_user_id
        where
            not exists (
                select *
                from purchase_check p
                where
                    p.unique_user_id = ac1.unique_user_id
                    and p.event_date = ac1.event_date
                    and p.customer_type = 'RETURN'
            )
            and activity1 = 0
            and activity2 = 0
    ),
    shp_data as (
        select
            shopify_order_id,
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
                from {{ source("ga4", "analytics_intraday_344079407__view") }} ga
                where
                    trim(ga.ecommerce_transaction_id) = shp.shopify_order_id
                    and ga.event_date > (
                        select coalesce(max(event_date), '1900-01-01')
                        from {{ ref("tbl_cl_ga4_sessions") }}
                    )
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
            trim(event_name) as event_name,
            trim(user_id) as user_id,
            trim(user_pseudo_id::string) as user_pseudo_id,
            is_active_user,
            event_params,
            trim(platform) as platform,
            trim(device_category) as device_category,
            trim(device_mobile_brand_name) as device_mobile_brand_name,
            trim(device_mobile_model_name) as device_mobile_model_name,
            trim(device_operating_system) as device_operating_system,
            trim(device_operating_system_version) as device_operating_system_version,
            trim(device_language) as device_language,
            trim(device_is_limited_ad_tracking) as device_is_limited_ad_tracking,
            trim(device_web_info_browser) as device_web_info_browser,
            trim(device_web_info_browser_version) as device_web_info_browser_version,
            trim(geo_country) as geo_country,
            trim(geo_region) as geo_region,
            trim(geo_city) as geo_city,
            trim(geo_metro) as geo_metro,
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
            trim(collected_traffic_source_gclid) as collected_traffic_source_gclid,
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
            case
                when trim(ecommerce_transaction_id) = '(not set)'
                then null
                else trim(ecommerce_transaction_id)
            end as shopify_order_id,
            coalesce(shp.quantity, ecommerce_total_item_quantity) total_item_quantity,
            ecommerce_purchase_revenue_in_usd,
            shp.total_tax tax_value,
            shp.shipping shipping_value,
            shp.gross_sales,
            shp.gsnd,
            shp.gmv,
            row_number() over (order by event_timestamp) as s_no
        from {{ source("ga4", "analytics_intraday_344079407__view") }} ga
        left join
            shp_data shp
            on trim(ga.ecommerce_transaction_id) = shp.shopify_order_id
            and trim(event_name) = 'purchase'
        where
            event_date > (
                select coalesce(max(event_date), '1900-01-01')
                from {{ ref("tbl_cl_ga4_sessions") }}
            )
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
    cte_eng_time as (
        select
            s_no,
            event_timestamp,
            ifnull("'engagement_time_msec'", 0) as engagement_time_msec
        from
            event_params
            pivot (max(event_value) for event_key in ('engagement_time_msec'))
    ),
    sessions as (
        select
            event_date,
            min(event_timestamp) event_timestamp,
            min(s_no) as s_no,
            max(user_id) as user_id,
            max(user_pseudo_id) as user_pseudo_id,
            coalesce(user_pseudo_id, user_id) unique_user_id,
            max("'ga_session_id'") as ga_session_id,
            coalesce(
                "'ga_session_id'" || ':' || user_pseudo_id,
                unique_user_id,
                "'ga_session_id'"
            ) as unique_session_id,
            is_active_user,
            sum(ifnull("'engagement_time_msec'", 0)) as engagement_time_msec,
            "'visitor_type'" as visitor_type,
            sum(
                case when event_name = 'view_search_results' then 1 else 0 end
            ) as view_search_results,
            sum(case when event_name = 'view_item' then 1 else 0 end) as view_item,
            sum(
                case when event_name = 'view_item_list' then 1 else 0 end
            ) as view_item_list,
            sum(case when event_name = 'select_item' then 1 else 0 end) as select_item,
            sum(case when event_name = 'login' then 1 else 0 end) as login,
            sum(case when event_name = 'page_view' then 1 else 0 end) as page_view,
            sum(case when event_name = 'add_to_cart' then 1 else 0 end) as add_to_cart,
            sum(
                case when event_name = 'remove_from_cart' then 1 else 0 end
            ) as remove_from_cart,
            sum(
                case when event_name = 'begin_checkout' then 1 else 0 end
            ) as checkouts_initiated,
            sum(
                case when event_name = 'add_shipping_info' then 1 else 0 end
            ) as add_shipping_info,
            sum(
                case when event_name = 'add_payment_info' then 1 else 0 end
            ) as add_payment_info,
            platform,
            device_category,
            device_mobile_brand_name,
            device_mobile_model_name,
            device_operating_system,
            device_operating_system_version,
            device_language,
            device_is_limited_ad_tracking,
            device_web_info_browser,
            device_web_info_browser_version,
            geo_country,
            geo_region,
            geo_city,
            geo_metro,
            {{ udf_cl_ga_source("traffic_source_source") }} traffic_source_source,
            {{ udf_cl_ga_medium("traffic_source_medium") }} traffic_source_medium,
            traffic_source_name,
            {{ udf_cl_ga_source("collected_traffic_source_manual_source") }} collected_traffic_source_manual_source,
            {{ udf_cl_ga_medium("collected_traffic_source_manual_medium") }} collected_traffic_source_manual_medium,
            collected_traffic_source_manual_campaign_id,
            collected_traffic_source_manual_campaign_name,
            collected_traffic_source_manual_source_platform,
            collected_traffic_source_gclid,
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
            count(distinct shopify_order_id) as orders,
            sum(total_item_quantity) as total_item_quantity,
            sum(ecommerce_purchase_revenue_in_usd) as ecommerce_purchase_revenue_in_usd,
            sum(tax_value) as tax_value,
            sum(shipping_value) as shipping_value,
            sum(gross_sales) as gross_sales,
            sum(gsnd) as gsnd,
            sum(gmv) as gmv,
            max(s_no) as last_s_no
        from
            event_params pivot (
                max(event_value) for event_key in (
                    'ga_session_id',
                    'engagement_time_msec',
                    'visitor_type',
                    'session_engaged',
                    'engaged_session_event'
                )
            )
        group by
            1,
            6,
            8,
            9,
            11,
            23,
            24,
            25,
            26,
            27,
            28,
            29,
            30,
            31,
            32,
            33,
            34,
            35,
            36,
            37,
            38,
            39,
            40,
            41,
            42,
            43,
            44,
            45,
            46,
            47,
            48,
            49,
            50
    ),
    sessiondata as (
        select
            *,
            lag(event_timestamp) over (
                partition by event_date, unique_session_id, unique_user_id
                order by event_timestamp
            ) as previous_timestamp
        from sessions
    ),
    sessionwithdiff as (
        select
            *,
            case
                when previous_timestamp is null
                then 1
                when timestampdiff(minute, previous_timestamp, event_timestamp) > 30
                then 1
                else 0
            end as newsession,
            sum(newsession) over (
                partition by event_date, unique_session_id, unique_user_id
                order by event_timestamp asc
                rows between unbounded preceding and current row
            ) as newsession_grp
        from sessiondata
    ),
    sessiongroup as (
        select
            sessionwithdiff.*,
            row_number() over (
                partition by
                    event_date, unique_session_id, unique_user_id, newsession_grp
                order by event_timestamp asc, sessionwithdiff.s_no asc
            ) as minn,
            row_number() over (
                partition by
                    event_date, unique_session_id, unique_user_id, newsession_grp
                order by event_timestamp desc, sessionwithdiff.s_no desc
            ) as maxx
        from sessionwithdiff
    ),
    final as (
        select
            event_date,
            min(event_timestamp) session_start_timestamp,
            hour(session_start_timestamp) as hourofday,
            max(last_s_no) as last_event_s_no,
            max(user_id) as user_id,
            max(user_pseudo_id) as user_pseudo_id,
            unique_user_id,
            min(
                case when minn = 1 then is_active_user end
            ) as first_event_is_active_user,
            max(
                case when maxx = 1 then is_active_user end
            ) as last_event_is_active_user,
            max(ga_session_id) as ga_session_id,
            unique_session_id ,
            newsession_grp as session_index,
            cast(sum(engagement_time_msec) as numeric(38, 0)) engagement_time_msec,
            min(case when minn = 1 then visitor_type end) as first_event_visitor_type,
            max(case when maxx = 1 then visitor_type end) as last_event_visitor_type,
            sum(view_search_results) as view_search_results,
            sum(view_item) as view_item,
            sum(view_item_list) as view_item_list,
            sum(select_item) as select_item,
            sum(login) as login,
            sum(page_view) as page_view,
            sum(add_to_cart) as add_to_cart,
            sum(remove_from_cart) as remove_from_cart,
            sum(checkouts_initiated) as checkouts_initiated,
            sum(add_shipping_info) as add_shipping_info,
            sum(add_payment_info) as add_payment_info,
            min(case when minn = 1 then platform end) as first_event_platform,
            max(case when maxx = 1 then platform end) as last_event_platform,
            min(
                case when minn = 1 then device_category end
            ) as first_event_device_category,
            max(
                case when maxx = 1 then device_category end
            ) as last_event_device_category,
            min(
                case when minn = 1 then device_mobile_brand_name end
            ) as first_event_device_mobile_brand_name,
            max(
                case when maxx = 1 then device_mobile_brand_name end
            ) as last_event_device_mobile_brand_name,
            min(
                case when minn = 1 then device_mobile_model_name end
            ) as first_event_device_mobile_model_name,
            max(
                case when maxx = 1 then device_mobile_model_name end
            ) as last_event_device_mobile_model_name,
            min(
                case when minn = 1 then device_operating_system end
            ) as first_event_device_operating_system,
            max(
                case when maxx = 1 then device_operating_system end
            ) as last_event_device_operating_system,
            min(
                case when minn = 1 then device_operating_system_version end
            ) as first_event_device_operating_system_version,
            max(
                case when maxx = 1 then device_operating_system_version end
            ) as last_event_device_operating_system_version,
            min(
                case when minn = 1 then device_language end
            ) as first_event_device_language,
            max(
                case when maxx = 1 then device_language end
            ) as last_event_device_language,
            min(
                case when minn = 1 then device_is_limited_ad_tracking end
            ) as first_event_device_is_limited_ad_tracking,
            max(
                case when maxx = 1 then device_is_limited_ad_tracking end
            ) as last_event_device_is_limited_ad_tracking,
            min(
                case when minn = 1 then device_web_info_browser end
            ) as first_event_device_web_info_browser,
            max(
                case when maxx = 1 then device_web_info_browser end
            ) as last_event_device_web_info_browser,
            min(
                case when minn = 1 then device_web_info_browser_version end
            ) as first_event_device_web_info_browser_version,
            max(
                case when maxx = 1 then device_web_info_browser_version end
            ) as last_event_device_web_info_browser_version,
            min(case when minn = 1 then geo_country end) as first_event_geo_country,
            max(case when maxx = 1 then geo_country end) as last_event_geo_country,
            min(case when minn = 1 then geo_region end) as first_event_geo_region,
            max(case when maxx = 1 then geo_region end) as last_event_geo_region,
            min(case when minn = 1 then geo_city end) as first_event_geo_city,
            max(case when maxx = 1 then geo_city end) as last_event_geo_city,
            min(case when minn = 1 then geo_metro end) as first_event_geo_metro,
            max(case when maxx = 1 then geo_metro end) as last_event_geo_metro,
            min(
                case when minn = 1 then traffic_source_source end
            ) as first_event_traffic_source_source,
            max(
                case when maxx = 1 then traffic_source_source end
            ) as last_event_traffic_source_source,
            min(
                case when minn = 1 then traffic_source_medium end
            ) as first_event_traffic_source_medium,
            max(
                case when maxx = 1 then traffic_source_medium end
            ) as last_event_traffic_source_medium,
            min(
                case when minn = 1 then traffic_source_name end
            ) as first_event_traffic_source_name,
            max(
                case when maxx = 1 then traffic_source_name end
            ) as last_event_traffic_source_name,
            min(
                case when minn = 1 then collected_traffic_source_manual_source end
            ) as first_event_collected_traffic_source_manual_source,
            max(
                case when maxx = 1 then collected_traffic_source_manual_source end
            ) as last_event_collected_traffic_source_manual_source,
            min(
                case when minn = 1 then collected_traffic_source_manual_medium end
            ) as first_event_collected_traffic_source_manual_medium,
            max(
                case when maxx = 1 then collected_traffic_source_manual_medium end
            ) as last_event_collected_traffic_source_manual_medium,
            min(
                case when minn = 1 then collected_traffic_source_manual_campaign_id end
            ) as first_event_collected_traffic_source_manual_campaign_id,
            max(
                case when maxx = 1 then collected_traffic_source_manual_campaign_id end
            ) as last_event_collected_traffic_source_manual_campaign_id,
            min(
                case
                    when minn = 1 then collected_traffic_source_manual_campaign_name
                end
            ) as first_event_collected_traffic_source_manual_campaign_name,
            max(
                case
                    when maxx = 1 then collected_traffic_source_manual_campaign_name
                end
            ) as last_event_collected_traffic_source_manual_campaign_name,
            min(
                case
                    when minn = 1 then collected_traffic_source_manual_source_platform
                end
            ) as first_event_collected_traffic_source_manual_source_platform,
            max(
                case
                    when maxx = 1 then collected_traffic_source_manual_source_platform
                end
            ) as last_event_collected_traffic_source_manual_source_platform,
            min(
                case when minn = 1 then collected_traffic_source_gclid end
            ) as first_event_collected_traffic_source_gclid,
            max(
                case when maxx = 1 then collected_traffic_source_gclid end
            ) as last_event_collected_traffic_source_gclid,
            min(
                case
                    when minn = 1
                    then session_traffic_source_last_click_manual_campaign_source
                end
            ) as first_event_session_traffic_source_last_click_manual_campaign_source,
            max(
                case
                    when maxx = 1
                    then session_traffic_source_last_click_manual_campaign_source
                end
            ) as last_event_session_traffic_source_last_click_manual_campaign_source,
            min(
                case
                    when minn = 1
                    then session_traffic_source_last_click_manual_campaign_medium
                end
            ) as first_event_session_traffic_source_last_click_manual_campaign_medium,
            max(
                case
                    when maxx = 1
                    then session_traffic_source_last_click_manual_campaign_medium
                end
            ) as last_event_session_traffic_source_last_click_manual_campaign_medium,
            min(
                case
                    when minn = 1
                    then session_traffic_source_last_click_manual_campaign_campaign_id
                end
            )
            as first_event_session_traffic_source_last_click_manual_campaign_campaign_id,
            max(
                case
                    when maxx = 1
                    then session_traffic_source_last_click_manual_campaign_campaign_id
                end
            )
            as last_event_session_traffic_source_last_click_manual_campaign_campaign_id,
            min(
                case
                    when minn = 1
                    then session_traffic_source_last_click_manual_campaign_campaign_name
                end
            )
            as first_event_session_traffic_source_last_click_manual_campaign_campaign_name,
            max(
                case
                    when maxx = 1
                    then session_traffic_source_last_click_manual_campaign_campaign_name
                end
            )
            as last_event_session_traffic_source_last_click_manual_campaign_campaign_name,
            min(
                case
                    when minn = 1
                    then
                        session_traffic_source_last_click_manual_campaign_source_platform
                end
            )
            as first_event_session_traffic_source_last_click_manual_campaign_source_platform,
            max(
                case
                    when maxx = 1
                    then
                        session_traffic_source_last_click_manual_campaign_source_platform
                end
            )
            as last_event_session_traffic_source_last_click_manual_campaign_source_platform,
            sum(orders) as orders,
            sum(total_item_quantity) as total_item_quantity,
            sum(ecommerce_purchase_revenue_in_usd) ecommerce_purchase_revenue_in_usd,
            sum(tax_value) as tax_value,
            sum(shipping_value) as shipping_value,
            sum(gross_sales) as gross_sales,
            sum(gsnd) as gsnd,
            sum(gmv) as gmv
        from sessiongroup
        group by event_date, unique_session_id, unique_user_id, newsession_grp
    )
select
    f.*,
    dateadd(
        'millisecond', cte_eng_time.engagement_time_msec, cte_eng_time.event_timestamp
    ) as session_end_timestamp,
    datediff(
        'millisecond', session_start_timestamp, session_end_timestamp
    ) time_on_site,
    case
        when
            (
                sum(page_view) over (
                    partition by f.event_date, f.unique_session_id, f.session_index
                )
                > 1
                or sum(f.engagement_time_msec) over (
                    partition by f.event_date, f.unique_session_id, f.session_index
                )
                > 10000
                or sum(orders) over (
                    partition by f.event_date, f.unique_session_id, f.session_index
                )
                > 0
            )
        then 1
        else 0
    end as engaged_session_flag,
    case
        when nu.unique_user_id is not null then 'NEW' else 'RETURNING'
    end as visitor_flag,
    {{
        udf_acquisition_channel_grouping(
            udf_coalesce(
                "first_event_traffic_source_source",
                "first_event_collected_traffic_source_manual_source",
            ),
            udf_coalesce(
                "first_event_traffic_source_medium",
                "first_event_collected_traffic_source_manual_medium",
            ),
            udf_coalesce(
                "first_event_traffic_source_name",
                "first_event_collected_traffic_source_manual_campaign_name",
            ),
        )
    }} as first_event_acquisition_channelgroup_nobull24,
    {{
        udf_acquisition_channel_grouping(
            udf_coalesce(
                "last_event_traffic_source_source",
                "last_event_collected_traffic_source_manual_source",
            ),
            udf_coalesce(
                "last_event_traffic_source_medium",
                "last_event_collected_traffic_source_manual_medium",
            ),
            udf_coalesce(
                "last_event_traffic_source_name",
                "last_event_collected_traffic_source_manual_campaign_name",
            ),
        )
    }} as last_event_acquisition_channelgroup_nobull24,
    {{
        udf_session_channel_grouping(
            "first_event_collected_traffic_source_manual_source",
            "first_event_collected_traffic_source_manual_medium",
            "first_event_collected_traffic_source_manual_campaign_name",
            "first_event_collected_traffic_source_manual_source_platform",
        )
    }} as first_event_session_channelgroup_nobull24,
    {{
        udf_session_channel_grouping(
            "last_event_collected_traffic_source_manual_source",
            "last_event_collected_traffic_source_manual_medium",
            "last_event_collected_traffic_source_manual_campaign_name",
            "last_event_collected_traffic_source_manual_source_platform",
        )
    }} as last_event_session_channelgroup_nobull24
from final f
left join
    new_users nu
    on f.unique_user_id = nu.unique_user_id
    and f.event_date = nu.event_date
left join cte_eng_time on f.last_event_s_no = cte_eng_time.s_no
