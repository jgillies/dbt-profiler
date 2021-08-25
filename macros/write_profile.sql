{% macro save_profile(dir, relation_name, schema=none, max_rows=none, max_columns=7, max_column_width=30, max_precision=none) %}

{%- set results = dbt_profiler.get_profile(relation_name, schema=schema) -%}
{%- set path=dir~relation_name~".csv" -%}


{% if execute %}
    {% set field_count = results[0]|length %}
    {% set g = namespace(comm0 = '', comma = '') -%}
    {% set header="column_name\ttype\tnot_null_proportion\tdistinct_proportion\tdistinct_count\tis_distinct\tmax_value\tmin_value\tprofiled_at\tcolumn_order" %}

    {# Rows #}
    {%- for record in results -%}
        {% if loop.first %}
           {{ log(header, info=True) }} 
        {% endif %}

        {%- set g.comma = '' -%}
        {% set g.r="" %}
        {%- for ix in record -%}

            {% set g.r=g.r ~ g.comma ~ ix|replace("\n", " ")|replace("\t", " ") %}
            {{- g.comma -}}
            {%- set g.comma = '\t ' -%}
            {% if loop.last %}
            {% endif %}

        {%- endfor %}
        {% set g.r=g.r ~ loop.index %}
        {{- log(g.r, info=True) -}}
    {% endfor %}
{% endif %}

{% endmacro %}
