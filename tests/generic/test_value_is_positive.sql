{% test value_is_positive(model, column_name) %}

with validation as(
    select {{column_name}} as total_value
    from {{model}}
),

validation_error as (
    select total_value
    from validation
    where total_value < 0
)

select * from validation_error

{% endtest %}