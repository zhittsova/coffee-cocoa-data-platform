-- depends_on: {{ ref('stg_trade_observations') }}

{% set metric_rows = [] %}
{% for node in graph.nodes.values() %}
    {% if node.resource_type == 'model' and node.config.meta.get('metrics') %}
        {% for metric in node.config.meta.get('metrics') %}
            {% do metric_rows.append({
                'metric_name': metric['name'],
                'model_name': node.name,
                'expression': metric['expression'],
                'metric_unit': metric['unit'],
                'denominator': metric['denominator'],
                'coverage': metric['coverage']
            }) %}
        {% endfor %}
    {% endif %}
{% endfor %}

{% if metric_rows %}
    {% for metric in metric_rows | sort(attribute='metric_name') %}
        select
            '{{ metric["metric_name"] | replace("'", "''") }}' as metric_name,
            '{{ metric["model_name"] | replace("'", "''") }}' as model_name,
            '{{ metric["expression"] | replace("'", "''") }}' as metric_expression,
            '{{ metric["metric_unit"] | replace("'", "''") }}' as metric_unit,
            '{{ metric["denominator"] | replace("'", "''") }}' as denominator,
            '{{ metric["coverage"] | replace("'", "''") }}' as coverage
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
{% else %}
    select
        cast(null as varchar) as metric_name,
        cast(null as varchar) as model_name,
        cast(null as varchar) as metric_expression,
        cast(null as varchar) as metric_unit,
        cast(null as varchar) as denominator,
        cast(null as varchar) as coverage
    where false
{% endif %}
