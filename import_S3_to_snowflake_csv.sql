{% macro import_s3_to_snowflake_csv(bucket,folder,pattern,model,table,file_format={},copy_options={}) %}
{# 
    bucket:
        s3 url, eg. s3://<bucket_name>-<domain>
    folder:
        s3 prefix/path for files, eg. <folder>/<subfolder>
    pattern:
        regex for files on s3 to ingest
            examples:
                '.*'        all files with prefix defined in "folder"
                '.*[.]csv'  all files with extension '.csv' and prefix defined in "folder"
    model:
        use "this" keyword to pass in model relation object, database and schema for model relation will be used
            eg. if environment is PRD and model path = 'models/WMHS_CONTRACTS/STG/...',
                then: database = 'PRD_WMHS_CONTRACTS', schema = 'STG'
    table:
        string value for name of table where data from s3 should be copied to
            !! columns in table should match the columns in the s3 file EXACTLY !!
    lnd_insert:
        when true, calls macro 'stage_to_land.sql' -- assumes table of the same name as 'table' exists in the LND schema
        and has all the same columns as well as a column to store timestamp at time of insert (see timestamp_column).
        eg. if domain = 'PRD', model = 'models/WMHS_CONTRACTS/STG/...', and table = 'cde_data_wmhs':
            All data from PRD_WMHS_CONTRACTS.STG.cde_data_wmhsE will be inserted into PRD_WMHS_CONTRACTS.LND.cde_data_wmhs
            if LND table does not have all the columns represented in STG table, this will fail.
        default = false
    timestamp_column:
        string value representing name of timestamp column in table in the LND schema (see lnd_insert above)
        default = 'AUDIT_LOAD_DTM'
    file_format={}:
        dictionary object for file format values. Currently only supports NULL_IF values
        
        'null_if': string or list of string values to convert to null during ingestion
            eg. file_format={'null_if':''} -- fields with blank values will be converted to null during ingestion
            eg. file_format={'null_if':['','na','empty']} -- fields with blank values, or 'na' or 'empty' will be converted to null during ingestion
            when 'null_if' key is not present in file_format dictionary, the 'NULL_IF' line is omitted from 'FILE_FORMAT' under the copy into statement
                the snowflake default in this case will be '\N' -- See https://docs.snowflake.com/en/sql-reference/sql/copy-into-table

#}

    {% set location = 's3://{}/{}/'.format(bucket,folder) %}
    {% set database = model.database %}
    {% set schema = model.schema %}

    {% set null_if = '' %}
    {% if file_format.get('null_if', true) == false %}
        {%- set null_if = '' -%}
    {% elif file_format.get('null_if') is string %}
        {% set null_if = 'NULL_IF = ' ~ "'" ~ file_format.get('null_if') ~ "'," %}
    {% elif file_format.get('null_if') is iterable %}
        {% set null_conditions = [] %}
        {% for condition in file_format.get('null_if') %}
            {% do null_conditions.append("'" ~ condition ~ "'") %}
        {% endfor %}
        {% set null_if = 'NULL_IF = (' ~ null_conditions | join(', ') ~ '),' %}
    {% elif not file_format.get('null_if') %}
        {% set null_if = '' %}
    {% else %}
        {{ exceptions.raise_compiler_error("Invalid argument type: expected string or list of strings.") }}
    {% endif %}

{% set on_error = 'ABORT_STATEMENT' %}
    {% if copy_options.get('on_error', true) == false %}
        {%- set on_error = 'ABORT_STATEMENT' -%}
    {% elif copy_options.get('on_error') is string %}
        {% set on_error = copy_options.get('on_error') %}
    {% elif not copy_options.get('on_error') %}
        {% set on_error = 'ABORT_STATEMENT' %}
    {% else %}
        {{ exceptions.raise_compiler_error("Invalid argument type: expected string") }}
    {% endif %}



    {% set load_table = 'load_' ~ table %}

    EXECUTE IMMEDIATE $$
    DECLARE
        row_count NUMBER := 0;
    BEGIN
        CREATE OR REPLACE TABLE {{database}}.{{schema}}.{{load_table}} LIKE {{database}}.{{schema}}.{{table}};

        COPY INTO {{database}}.{{schema}}.{{load_table}}
        FROM '{{ location }}'
        storage_integration = {{ env_var("DBT_S3_TO_SNOWFLAKE_INT") }}
        PATTERN = '{{ pattern }}'
        FILE_FORMAT = (
            TYPE=csv,
            COMPRESSION=AUTO,
            FIELD_DELIMITER = ',',
            SKIP_HEADER=1,
            SKIP_BLANK_LINES=TRUE,
            TRIM_SPACE=TRUE,
            FIELD_OPTIONALLY_ENCLOSED_BY='"',
            {{ null_if }}
            ERROR_ON_COLUMN_COUNT_MISMATCH=TRUE,
            REPLACE_INVALID_CHARACTERS=TRUE,
            EMPTY_FIELD_AS_NULL=TRUE
        )
        ON_ERROR='{{ on_error }}'
        PURGE=FALSE
        TRUNCATECOLUMNS=FALSE
        FORCE=FALSE
        ;

        SELECT COUNT(*) INTO row_count FROM {{database}}.{{schema}}.{{load_table}};

        IF (row_count = 0) THEN
            DROP TABLE IF EXISTS {{database}}.{{schema}}.{{load_table}};
            RAISE STATEMENT_ERROR USING MESSAGE = 'S3 import aborted: no data found for {{location}} with pattern {{pattern}}';
        ELSE
            TRUNCATE TABLE {{database}}.{{schema}}.{{table}};
            INSERT INTO {{database}}.{{schema}}.{{table}}
            SELECT * FROM {{database}}.{{schema}}.{{load_table}};
            DROP TABLE IF EXISTS {{database}}.{{schema}}.{{load_table}};
        END IF;
    END;
    $$;

{% endmacro %}