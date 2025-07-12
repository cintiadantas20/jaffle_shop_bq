{% set old_etl_relation=ref('customer_orders') %} 

{% set dbt_relation=ref('fct_customer_orders') %}   

with audit_results as (
    {{
        audit_helper.compare_and_classify_relation_rows(
            a_relation=old_etl_relation,
            b_relation=dbt_relation,
            primary_key_columns = ['order_id'],
            columns = None,
            sample_limit = None
        ) }}
)

SELECT
    dbt_audit_row_status,
    COUNT(*) AS total_rows
FROM audit_results
GROUP BY 1;
