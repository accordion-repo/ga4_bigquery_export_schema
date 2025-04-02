{% macro udf_acquisition_channel_grouping(source, medium, campaign) %}

    case
        when
            {{ source }} like '%mntn%'
            or {{ source }} like '%tatari%'
            or {{ medium }} like '%tv%'
            or {{ medium }} like '%television_prospecting%'
        then 'TV'

        when
            {{ source }} like '%youtube%'
            or {{ campaign }} like '%youtube%'
            or {{ campaign }} like '%demand gen%'
            or {{ campaign }} like '%dgen%'
        then 'YouTube'

        when
            ({{ medium }} like '%ppc%' or {{ medium }} like '%cpc%')
            and {{ campaign }} like '%brand%'
            and {{ campaign }} not like '%performance max%'
        then 'Paid Brand Search'

        when
            (
                {{ medium }} like '%cpc%'
                or {{ medium }} like '%paid_shopping%'
                or {{ medium }} like '%paid_search%'
                or {{ campaign }} like '%pla_%'
                or {{ campaign }} like '%pmax%'
                or {{ campaign }} like '%search%'
                --or {{ sp }} like '%google%'
            )
            and (
                {{ campaign }} not like '%demand gen%'
                or {{ campaign }} not like '%dgen%'
                or {{ campaign }} not like '%brand%'
                or {{ campaign }} is null
            )
        then 'Paid Non-Brand Search'

        when
            (
                {{ source }} like '%facebook%'
                or {{ source }} like '%instagram%'
                or {{ source }} like '%meta%'
                or {{ source }} like '%igshopping%'
            )
            and {{ medium }} like '%paid%'
        then 'Paid Social'

        when {{ source }} like '%tiktok%' and {{ medium }} like '%paid%'
        then 'TikTok'

        when
            {{ source }} like '%email%'
            or {{ source }} like '%e-mail%'
            or {{ source }} like '%e_mail%'
            or {{ source }} like '%e mail%'
            or {{ source }} like '%bluecore%'
            or {{ source }} like '%emarsys%'
            or {{ medium }} like '%email%'
            or {{ medium }} like '%e-mail%'
            or {{ medium }} like '%e_mail%'
            or {{ medium }} like '%e mail%'
        then 'Email'

        when
            {{ source }} like '%attentive%'
            or {{ source }} like '%sms%'
            or {{ medium }} like '%text%'
            or {{ medium }} like '%sms%'
        then 'SMS'

        when
            (
                {{ source }} like '%impact%'
                or {{ source }} like '%api.id.me%'
                or {{ source }} like '%shop.id.me%'
                or {{ medium }} like '%affiliate%'
                or {{ medium }} like '%partnership%'
                or {{ campaign }} like '%nobull%'
            )
            and ({{ source }} not like '%spotify%' or {{ source }} not like '%tb12%')
        then 'Affiliates'

        when {{ source }} like '%spotify%' or {{ source }} like '%podcast%' or {{medium}} = 'audio'
        then 'Audio'

        when {{ source }} like '%twitter%' and {{ medium }} like '%paid social%'
        then 'Twitter'

        when {{ source }} like '%pinterest%' and {{ medium }} like '%paid social%'
        then 'Pinterest'

        when {{ medium }} like 'organic' --or {{ sp }} like '%shopping free listings%'
        then 'Organic Search'

        when
            (
                {{ source }} like '%facebook%'
                or {{ medium }} like '%social%'
                or {{ medium }} like '%social-network%'
                or {{ medium }} like '%social-media%'
                or {{ medium }} like '%sm%'
                or {{ medium }} like '%social network%'
                or {{ medium }} like '%social media%'
            )
            and {{ medium }} not like '%paid%'
        then 'Organic Social'

        when
            {{ source }} like '%narvar%'
            or {{ source }} like '%direct%'
            or {{ medium }} like '%direct%'
            or {{ medium }} like '%(not set)%'
            or {{ medium }} like '%(none)%'
        then 'Direct'

        when
            (
                {{ source }} like '%tb12%'
                or {{ source }} like '%klaviyo%'
                or {{ medium }} like '%referral%'
                or {{ medium }} like '%app%'
                or {{ medium }} like '%link%'
            )
            and ({{ source }} not like '%klarna%' or {{ source }} not like '%api.id.me%')
        then 'Referral'

        else 'Unassigned'
    end
{% endmacro %}
