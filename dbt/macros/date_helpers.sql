{% macro month_start(expression) %}
  {{ return(adapter.dispatch('month_start', 'gcp_retail_analytics')(expression)) }}
{% endmacro %}

{% macro default__month_start(expression) %}
  date_trunc('month', cast({{ expression }} as date))
{% endmacro %}

{% macro bigquery__month_start(expression) %}
  date_trunc(cast({{ expression }} as date), month)
{% endmacro %}

{% macro incremental_lookback_start() %}
  {{ return(adapter.dispatch('incremental_lookback_start', 'gcp_retail_analytics')()) }}
{% endmacro %}

{% macro default__incremental_lookback_start() %}
  date_trunc('month', current_date - interval '1 month')
{% endmacro %}

{% macro bigquery__incremental_lookback_start() %}
  date_trunc(date_sub(current_date(), interval 1 month), month)
{% endmacro %}

{% macro mart_partition_config() %}
  {{ return(adapter.dispatch('mart_partition_config', 'gcp_retail_analytics')()) }}
{% endmacro %}

{% macro default__mart_partition_config() %}
  {{ return(none) }}
{% endmacro %}

{% macro bigquery__mart_partition_config() %}
  {{ return({
    'field': 'order_month',
    'data_type': 'date',
    'granularity': 'month'
  }) }}
{% endmacro %}

{% macro mart_incremental_strategy() %}
  {{ return(adapter.dispatch('mart_incremental_strategy', 'gcp_retail_analytics')()) }}
{% endmacro %}

{% macro default__mart_incremental_strategy() %}
  {{ return('delete+insert') }}
{% endmacro %}

{% macro bigquery__mart_incremental_strategy() %}
  {{ return('insert_overwrite') }}
{% endmacro %}
