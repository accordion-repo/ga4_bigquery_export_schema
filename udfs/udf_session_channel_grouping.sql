{% macro udf_session_channel_grouping(ss, sm, sc, sp) %}
    case
        when
            {{ ss }} like '%mntn%'
            or {{ ss }} like '%tatari%'
            or {{ sm }} like '%tv%'
            or {{ sm }} like '%television_prospecting%'
        then 'TV'

        when
            {{ ss }} like '%youtube%'
            or {{ sc }} like '%youtube%'
            or {{ sc }} like '%demand gen%'
            or {{ sc }} like '%dgen%'
        then 'YouTube'

        when
            ({{ sm }} like '%ppc%' or {{ sm }} like '%cpc%')
            and {{ sc }} like '%brand%'
            and {{ sc }} not like '%performance max%'
        then 'Paid Brand Search'

        when
            (
                {{ sm }} like '%cpc%'
                or {{ sm }} like '%paid_shopping%'
                or {{ sm }} like '%paid_search%'
                or {{ sc }} like '%pla_%'
                or {{ sc }} like '%pmax%'
                or {{ sc }} like '%search%'
                or {{ sp }} like '%google%'
            )
            and (
                {{ sc }} not like '%demand gen%'
                or {{ sc }} not like '%dgen%'
                or {{ sc }} not like '%brand%'
                or {{ sc }} is null
            )
        then 'Paid Non-Brand Search'

        when
            (
                {{ ss }} like '%facebook%'
                or {{ ss }} like '%instagram%'
                or {{ ss }} like '%meta%'
                or {{ ss }} like '%igshopping%'
            )
            and {{ sm }} like '%paid%'
        then 'Paid Social'

        when {{ ss }} like '%tiktok%' and {{ sm }} like '%paid%'
        then 'TikTok'

        when
            {{ ss }} like '%email%'
            or {{ ss }} like '%e-mail%'
            or {{ ss }} like '%e_mail%'
            or {{ ss }} like '%e mail%'
            or {{ ss }} like '%bluecore%'
            or {{ ss }} like '%emarsys%'
            or {{ sm }} like '%email%'
            or {{ sm }} like '%e-mail%'
            or {{ sm }} like '%e_mail%'
            or {{ sm }} like '%e mail%'
        then 'Email'

        when
            {{ ss }} like '%attentive%'
            or {{ ss }} like '%sms%'
            or {{ sm }} like '%text%'
            or {{ sm }} like '%sms%'
        then 'SMS'

        when
            (
                {{ ss }} like '%impact%'
                or {{ ss }} like '%api.id.me%'
                or {{ ss }} like '%shop.id.me%'
                or {{ sm }} like '%affiliate%'
                or {{ sm }} like '%partnership%'
                or {{ sc }} like '%nobull%'
            )
            and ({{ ss }} not like '%spotify%' or {{ ss }} not like '%tb12%')
        then 'Affiliates'

        when {{ ss }} like '%spotify%' or {{ ss }} like '%podcast%' or {{sm}} = 'audio'
        then 'Audio'

        when {{ ss }} like '%twitter%' and {{ sm }} like '%paid social%'
        then 'Twitter'

        when {{ ss }} like '%pinterest%' and {{ sm }} like '%paid social%'
        then 'Pinterest'

        when {{ sm }} like 'organic' or {{ sp }} like '%shopping free listings%'
        then 'Organic Search'

        when
            (
                {{ ss }} like '%facebook%'
                or {{ sm }} like '%social%'
                or {{ sm }} like '%social-network%'
                or {{ sm }} like '%social-media%'
                or {{ sm }} like '%sm%'
                or {{ sm }} like '%social network%'
                or {{ sm }} like '%social media%'
            )
            and {{ sm }} not like '%paid%'
        then 'Organic Social'

        when
            {{ ss }} like '%narvar%'
            or {{ ss }} like '%direct%'
            or {{ sm }} like '%direct%'
            or {{ sm }} like '%(not set)%'
            or {{ sm }} like '%(none)%'
        then 'Direct'

        when
            (
                {{ ss }} like '%tb12%'
                or {{ ss }} like '%klaviyo%'
                or {{ sm }} like '%referral%'
                or {{ sm }} like '%app%'
                or {{ sm }} like '%link%'
            )
            and ({{ ss }} not like '%klarna%' or {{ ss }} not like '%api.id.me%')
        then 'Referral'

        else 'Unassigned'
    end
{% endmacro %}
