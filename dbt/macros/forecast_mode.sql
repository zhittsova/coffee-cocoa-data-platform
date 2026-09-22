{% macro assert_forecast_mode() %}
  {% set mode = var('forecast_availability_mode', 'retrospective_current_vintage') %}
  {% if mode != 'retrospective_current_vintage' %}
    {{ exceptions.raise_compiler_error('Forecast as_of mode requires verified historical release and capture evidence') }}
  {% endif %}
{% endmacro %}
