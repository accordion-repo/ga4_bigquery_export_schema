{% macro udf_acquisition_channel_grouping(ss, sm, sc) %}

    case
        -- TV
        when
            coalesce({{ ss }}, '') ilike '%mntn%'
            or coalesce({{ sm }}, '') ilike '%tv%'
            or coalesce({{ ss }}, '') ilike '%tatari%'
            or coalesce({{ sm }}, '') ilike '%television_prospecting%'
            or coalesce({{ ss }}, '') ilike '%ctv%'
        then 'TV'

        -- YouTube
        when
            coalesce({{ ss }}, '') ilike '%youtube%'
            or coalesce({{ sc }}, '') ilike '%youtube%'
            or coalesce({{ sc }}, '') ilike '%demandgen%'
            or coalesce({{ sc }}, '') ilike '%dgen%'
        then 'YouTube'

        -- Paid Brand Search
        when
            (
                (
                    coalesce({{ sm }}, '') ilike '%ppc%'
                    or coalesce({{ sm }}, '') ilike '%cpc%'
                )
                and coalesce({{ sc }}, '') ilike '%brand%'
                and coalesce({{ sc }}, '') not ilike '%performance max%'
            )
        then 'Paid Brand Search'

        -- Partnership
        when
            coalesce({{ sm }}, '') ilike '%partnership%'
            or coalesce({{ ss }}, '') ilike '%partnership%'
            or coalesce({{ sc }}, '') ilike '%NOBULL Athletes%'
            or coalesce({{ sc }}, '') ilike '%NOBULLInfluencers%'
        then 'Partnership'

        -- Affiliates
        when
            (
                coalesce({{ sm }}, '') ilike '%affiliate%'
                or coalesce({{ ss }}, '') ilike '%impact%'
                or coalesce({{ ss }}, '') ilike '%api.id.me%'
                or coalesce({{ ss }}, '') ilike '%shop.id.me%'
                or coalesce({{ sc }}, '') ilike '%Ebates%'
                or coalesce({{ sc }}, '') ilike '%BeFrugal.com%'
            )
            and not (
                coalesce({{ ss }}, '') ilike '%spotify%'
                or coalesce({{ ss }}, '') ilike '%tb12%'
            )
        then 'Affiliates'

        -- Paid Non-Brand Search
        when
            (
                coalesce({{ sm }}, '') ilike '%cpc%'
                or coalesce({{ sm }}, '') ilike '%paid_shopping%'
                or coalesce({{ sm }}, '') ilike '%paid_search%'
                -- COALESCE( {{ sp }} , '') ILIKE '%google ads%' or
                or coalesce({{ sc }}, '') ilike '%pla_%'
                or coalesce({{ sc }}, '') ilike '%pmax%'
                or coalesce({{ sc }}, '') ilike '%search%'
            )
            and not (
                coalesce({{ sc }}, '') ilike '%brand%'
                or coalesce({{ sc }}, '') ilike '%junky%'
                or coalesce({{ sc }}, '') ilike '%Ebates%'
            )
        then 'Paid Non-Brand Search'

        -- Paid Social
        when
            (
                coalesce({{ ss }}, '') ilike '%facebook%'
                or coalesce({{ ss }}, '') ilike '%instagram%'
                or coalesce({{ ss }}, '') ilike '%meta%'
                or coalesce({{ ss }}, '') ilike '%igshopping%'
                or coalesce({{ ss }}, '') = 'tinuiti'
            )
            and (coalesce({{ sm }}, '') ilike '%paid%')
        then 'Paid Social'

        -- TikTok
        when
            coalesce({{ ss }}, '') ilike '%tiktok%'
            and coalesce({{ sm }}, '') ilike '%paid%'
        then 'TikTok'  -- closed the missing paren

        -- Email (with GA4 logic)
        when
            (
                coalesce({{ ss }}, '') ilike '%email%'
                or coalesce({{ ss }}, '') ilike '%e-mail%'
                or coalesce({{ ss }}, '') ilike '%e_mail%'
                or coalesce({{ ss }}, '') ilike '%e mail%'
                or coalesce({{ sm }}, '') ilike '%email%'
                or coalesce({{ sm }}, '') ilike '%e-mail%'
                or coalesce({{ sm }}, '') ilike '%e_mail%'
                or coalesce({{ sm }}, '') ilike '%e mail%'
                or coalesce({{ ss }}, '') ilike '%bluecore%'
                or coalesce({{ ss }}, '') ilike '%emarsys%'
            )
        then 'Email'

        -- SMS
        when
            (
                coalesce({{ sm }}, '') ilike '%sms%'
                or coalesce({{ sm }}, '') ilike '%text%'
                or coalesce({{ ss }}, '') ilike '%attentive%'
                or coalesce({{ ss }}, '') = 'sms'
            )
        then 'SMS'

        -- Audio
        when
            coalesce({{ ss }}, '') ilike '%spotify%'
            or coalesce({{ ss }}, '') ilike '%podcast%'
            or coalesce({{ sm }}, '') ilike '%audio%'
        then 'Audio'

        -- Twitter
        when
            coalesce({{ ss }}, '') ilike '%twitter%'
            and coalesce({{ sm }}, '') ilike '%paid%'
        then 'Twitter'

        -- Pinterest
        when
            coalesce({{ ss }}, '') ilike '%pinterest%'
            and coalesce({{ sm }}, '') ilike '%paid%'
        then 'Pinterest'

        -- Organic Search + Shopping
        when
            (
                coalesce({{ sm }}, '') = 'organic'
                or coalesce({{ ss }}, '')
                rlike '.*360\\\\.cn.*|.*alice.*|.*aol.*|.*ask.*|.*auone.*|.*avg.*|.*babylon.*|.*baidu.*|.*biglobe.*|.*bing.*|.*centrum\\\\.cz.*|.*cnn.*|.*comcast.*|.*conduit.*|.*daum.*|.*dogpile.*|.*duckduckgo.*|.*ecosia\\\\.org.*|.*seznam.*|.*eniro.*|.*exalead\\\\.com.*|.*excite\\\\.com.*|.*firmy.*|.*globo.*|.*go\\\\.mail\\\\.ru.*|.*google.*|.*google-play.*|.*incredimail.*|.*kvasir.*|.*qwant.*|.*lycos.*|.*naver.*|.*sogou.*|.*yandex.*|.*rambler.*|.*msn.*|.*najdi.*|.*onet.*|.*rakuten.*|.*search.*|.*so\\\\.com.*|.*startsiden.*|.*terra.*|.*tut\\\\.by.*|.*ukr.*|.*virgilio.*|.*google shopping.*|.*igshopping.*|.*amazon.*|.*alibaba.*|.*shopify.*|.*stripe.*|.*ebay.*|.*etsy.*|.*mercadolibre.*|.*shopping.*|.*walmart.*|.*shop.*'
                or coalesce({{ sc }}, '') ilike '%shop%'
            )
        then 'Organic Search'

        -- Organic Social
        when
            (
                (
                    coalesce({{ ss }}, '')
                    rlike '.*facebook.*|.*instagram.*|.*twitter.*|.*linkedin.*|.*pinterest.*|.*reddit.*|.*igshopping.*|.*meta.*|.*tiktok.*|.*ig.*|.*social.*|.*snapchat.*'
                    or coalesce({{ sm }}, '') ilike '%social%'
                    or coalesce({{ sm }}, '') = 'sm'
                    or coalesce({{ ss }}, '') ilike '%x.com%'
                )
                and coalesce({{ sm }}, '') not ilike '%paid%'
            )
        then 'Organic Social'

        -- Direct
        when
            coalesce({{ ss }}, '') ilike '%narvar%'
            or coalesce({{ ss }}, '') ilike '%direct%'
            or coalesce({{ sm }}, '') ilike '%(not set)%'
            or coalesce({{ sm }}, '') ilike '%(none)%'
            or coalesce({{ sm }}, '') ilike '%direct%'
        then 'Direct'

        -- Referral
        when
            (
                coalesce({{ sm }}, '') ilike '%referral%'
                or coalesce({{ sm }}, '') ilike '%app%'
                or coalesce({{ sm }}, '') ilike '%link%'
                or coalesce({{ ss }}, '') ilike '%TB12%'
                or coalesce({{ ss }}, '') ilike '%Klaviyo%'
                or coalesce({{ ss }}, '') ilike '%breakaway%'
            )
            and not (
                coalesce({{ ss }}, '') ilike '%api.id.me%'
                or coalesce({{ ss }}, '') ilike '%klarna%'
            )
        then 'Referral'

        else 'Unassigned'
    end
{% endmacro %}
