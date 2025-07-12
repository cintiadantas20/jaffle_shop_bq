{% macro limit_data_in_dev(column_name, dev_days_of_data=3) %}
{% if target.name == 'dev'%}
where {{ column_name }} >= cast(date_add(current_timestamp(),  interval - {{ dev_days_of_data }} day) as date)
{% endif%}
{% endmacro %}