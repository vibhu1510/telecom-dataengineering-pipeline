{% macro generate_surrogate_key(field_list) %}
    {#
      Generates a surrogate key by MD5-hashing a concatenation of the given fields.
      Null values are coalesced to 'UNKNOWN' before hashing to ensure consistency.
      Compatible with Trino SQL dialect.
    #}
    md5(
        concat(
            {% for field in field_list %}
                coalesce(cast({{ field }} as varchar), 'UNKNOWN')
                {% if not loop.last %}, '|', {% endif %}
            {% endfor %}
        )
    )
{% endmacro %}
