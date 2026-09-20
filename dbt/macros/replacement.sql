{% macro replacement_predicate(dataset, alias='') %}
    {% set scopes = var('replacement_scopes', {}).get(dataset, none) %}
    {% set prefix = alias ~ '.' if alias else '' %}
    {% if scopes is none %}
        true
    {% elif not scopes %}
        false
    {% else %}
        (
        {% for scope in scopes %}
            (
                {{ prefix }}period_month between '{{ scope['start'] }}-01'::date
                and '{{ scope['end'] }}-01'::date
                {% for column, values in scope['keys'].items() %}
                    and {{ prefix }}{{ adapter.quote(column) }} in (
                    {% for value in values %}
                        '{{ value | replace("'", "''") }}'{% if not loop.last %},{% endif %}
                    {% endfor %}
                    )
                {% endfor %}
            ){% if not loop.last %} or {% endif %}
        {% endfor %}
        )
    {% endif %}
{% endmacro %}

{% macro delete_replacement_scope(dataset) %}
    {% if is_incremental() %}
        delete from {{ this }} where {{ replacement_predicate(dataset) }}
    {% endif %}
{% endmacro %}
