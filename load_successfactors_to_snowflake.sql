{% set aws_domain = "wmoraclecloud-" ~ env_var("DBT_AWS_DOMAIN") %}

{{
    config(
        materialized="table",
        pre_hook=[
            import_s3_to_snowflake_csv(
                aws_domain,                
				"HCM/DEV3/WMHS_HCM/INB/PRE/SUCCESSFACTORS",
                '.*[.]csv',
                this,
                "STG_WMHS_SUCCESSFACTORS_DETAILS",
                file_format={
                    "null_if": ["(None identified)", "", "(None Identified)"]
                },
                copy_options={"on_error": "SKIP_FILE"},
            )
        ],
        post_hook=["DROP TABLE IF EXISTS load_stg_wmhs_successfactors_details"],
    )
}}


select count(*) as row_count
from {{ source('wmhs_stg', 'STG_WMHS_SUCCESSFACTORS_DETAILS') }}