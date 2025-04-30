{% macro udf_cl_ga_medium(medium) %}
    case
        when {{ medium }} = 'aff' or {{ medium }} like '%affiliate%'
        then 'affiliate'

        when {{ medium }} = 'shop' or {{ medium }} = 'shop_web'
        then 'shop'

        when {{ medium }} = 'ema' or {{ medium }} like '%email%'
        then 'email'

        when {{ medium }} like '%organic instagram%'
        then 'organic instagram'

        when {{ medium }} like 'web%'
        then 'web'

        when {{ medium }} like '%partnership%'
        then 'partnership'

        when
            {{ medium }} like '%directmail%'
            or {{ medium }} like '%direct_mail%'
            or {{ medium }} like '%direct mail%'
        then 'direct mail'

        when
            {{ medium }} like '%paid social%'
            or {{ medium }} like '%paid_social%'
            or {{ medium }} like '%paid-social%'
            or {{ medium }} like '%paidsocial%'
        then 'paid social'

        when
            {{ medium }} like '%social feed%'
            or {{ medium }} like '%social_feed%'
            or {{ medium }} like '%socialfeed%'
        then 'social feed'

        when
            {{ medium }} like '%social media%'
            or {{ medium }} like '%social_media%'
            or {{ medium }} like '%socialmedia%'
        then 'social media'

        when {{ medium }} like '%koala%'
        then 'koala inspector'

        when {{ medium }} = 'data not available'
        then null

        else {{ medium }}
    end
{% endmacro %}
