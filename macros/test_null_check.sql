{% macro test_null_check(model, column_name) %}

select * from {{model}} where {{column_name}} is null

{% endmacro %}