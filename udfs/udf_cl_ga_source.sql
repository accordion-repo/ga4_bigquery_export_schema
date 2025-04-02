{% macro udf_cl_ga_source(source) %}
    case
        when {{ source }} like '%fb pixel%'
          or {{ source }} like '%facebook%'
          or {{ source }} = 'fb'
          or {{ source }} like '%fb-ad%' 
        then 'facebook'
        
        when {{ source }} like '%yahoo%' 
        then 'yahoo'
        
        when {{ source }} like '%google%'
          or {{ source }} like '%adwords%'
        then 'google'
        
        when {{ source }} like '%youtube%' 
        then 'youtube'
        
        when {{ source }} like '%tb12%' 
        then 'tb12'
        
        when {{ source }} like '%tiktok%' 
        then 'tiktok'
        
        when {{ source }} like '%yandex%' 
        then 'yandex'

        when {{ source }} like '%wikipedia%' 
        then 'wikipedia'
        
        else {{ source }}
    end
{% endmacro %}
