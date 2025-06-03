{% macro qualified_alias_columns(source_model, alias, prefix) %}
    {% set relation = ref(source_model) %}
    {% set columns = adapter.get_columns_in_relation(relation) %}
    {% for column in columns %}
        {{ alias }}.{{ column.name }} AS {{ prefix }}_{{ column.name }}{% if not loop.last %},{% endif %}
    {% endfor %}
{% endmacro %}
