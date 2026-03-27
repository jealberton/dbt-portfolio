{%- set data_layer = 'gold' %}
{%- set data_source = 'client_mbr' %}
{%- set table_name = get_table_name() %}
{%- set run_override = var('gld_client_mbr__snapshot_override', false) %}
{%- set labels = get_labels(
    labels=['global',data_layer],
    custom_labels={
        'automated_by': 'data-engineer', 
        'data_source': data_source,
        'environment': target.target_name,
    }
)%}

{{
    config(
        labels = labels,
        materialized='incremental',
        alias = table_name,
        on_schema_change='append_new_columns',
        unique_key=['snapshot_date', 'program_code', 'country','last_snapshot_ts_est'],
        partition_by={
            "field": "snapshot_date",
            "data_type": "date",
            "granularity": "day"
        },
        incremental_strategy='merge',
        incremental_predicates=[
            "DBT_INTERNAL_DEST.snapshot_date = current_date('America/New_York')"
        ]
    )
}}

{% if should_full_refresh() and target.name != 'dev' %}
  {% do exceptions.raise_compiler_error(
    "Full refresh is not allowed for this model. Please use direct DDL/DML on this table if you want to make changes") 
  %}
{% endif %}


with 
    european_countries as (
        -- CTE 0: Defines the country codes for European aggregation (EUR)
        select 'DEU' as country_code
        union all select 'ESP'
        union all select 'FRA'
        union all select 'GBR'
        union all select 'ITA'
    ),

    program_european_counts as (
        -- CTE 0.1: Counts the number of European countries involved in each program
        select
            program_code,
            count(distinct country) as num_european_countries
        from {{ ref('gld_client_mbr__client_mbr_summary') }}
        where country in (select country_code from european_countries)
        group by program_code
    ),

    /* ---------------------------------------------------------------------
    1. FILTERED_HISTORY_FOR_LAG (MoM BASELINE DATA)
    CRITICAL FIX: All columns are aliased to snake_case for consistency,
    solving the "Unrecognized name" error upon incremental runs.
    --------------------------------------------------------------------- */
    filtered_history_for_lag as (
    select
        -- 1. Snapshot and Reference Fields
        snapshot_date,
        mbr_reference,
        {% if is_incremental() %}
            phase,
            program_name,           
            program_code,           
            country,
            start_date,             
            end_date,               
            months_since_completion,

            -- 2. Core Metrics (Integers)
            admitted,
            enrolled_or_graduated,  
            dropped_out,            
            placed_fellows,         
            beyond,
            
            -- Conditional Columns 
            direct_beyond,     
            indirect_beyond,    

            internal_hire,          
            external_qualifying,    
            internal_training,      

            -- 3. Rate Metrics (Floats)
            placement_rate,         
            retention_rate,         
            run_rate,               

            -- 4. Certificate Fields
            certification_name,     
            certification_attempted,
            certification_received,   
            percent_exam_attempted, 
            percent_of_graduated,   
            job_transitions_with_certificate, 
            percent_of_job_transitions_with_certificate, 
            external_qualifying_with_certificate, 
            internal_hire_with_certificate, 
            beyond_with_certificate,
            internal_training_with_certificate, 
            cert_eligible,          

            -- 6. Financials and NPS
            program_revenue,        
            cost_per_placement,     
            cost_per_placement_without_beyond, 
            program_nps,            
            
            -- 7. New Placed/Beyond Columns
            placed_without_indirect, 
            beyond_without_indirect, 
            last_snapshot_ts_est
        {% else %}
            Phase as phase,
            Program_name as program_name,           
            Program_Code as program_code,           
            Country as country,
            Start_date as start_date,             
            End_Date as end_date,               
            Months_Since_Completion as months_since_completion,

            -- 2. Core Metrics (Integers)
            Admitted as admitted,
            Enrolled as enrolled_or_graduated,  
            Dropped_Out as dropped_out,            
            Placed_Fellows as placed_fellows,         
            Beyond as beyond,
            
            -- Conditional Columns 
            null as direct_beyond,
            null as indirect_beyond,

            Internal_Hire as internal_hire,          
            External_Qualifying as external_qualifying,    
            Internal_Training as internal_training,      

            -- 3. Rate Metrics (Floats)
            Placement_Rate as placement_rate,         
            Retention_Rate as retention_rate,         
            Run_Rate as run_rate,               

            -- 4. Certificate Fields
            Certificate_Name as certification_name,     
            Certification_Attempted as certification_attempted,
            Certification_Received as certification_received,   
            Percent_Exam_Attempted as percent_exam_attempted, 
            Percent_Of_Graduated as percent_of_graduated,   
            Job_transitions_with_Certificate as job_transitions_with_certificate, 
            Percent_Of_Job_transitions_with_Certificate as percent_of_job_transitions_with_certificate, 
            External_Qualifying_with_Certificate as external_qualifying_with_certificate, 
            Internal_Hire_with_Certificate as internal_hire_with_certificate, 
            Beyond_with_Certificate as beyond_with_certificate,
            Internal_Training_with_Certificate as internal_training_with_certificate, 
            Cert_Eligible as cert_eligible,          

            -- 6. Financials and NPS
            Program_Revenue as program_revenue,        
            Cost_Per_Placement as cost_per_placement,     
            Cost_Per_Placement_Without_Beyond as cost_per_placement_without_beyond, 
            Program_NPS as program_nps,            
            
            -- 7. New Placed/Beyond Columns
            null as placed_without_indirect, 
            null as beyond_without_indirect,
            last_snapshot_ts_est
        {% endif %}
        
    from 
    {% if is_incremental() %}
        {{ this }}
    {% else %}
       {{ source('brz_client_mbr__backfill_sheet', 'mbr_history_backfill') }}
    {% endif %}

    where 1=1
    -- CRITICAL FIX 1: Filter out the current month from the historical data (backfill or incremental).
    -- This ensures the LAG calculation works (M-1) and prevents duplication on Full Refresh.
    and parse_date('%B %Y', mbr_reference) < date_trunc(current_date('America/New_York'), MONTH)

),

    /* ---------------------------------------------------------------------
    2. CURRENT_MBR_BASE
    Builds today's MBR base data from the current summary table.
    All output columns are snake_case.
    --------------------------------------------------------------------- */
    mbr_summary_base as (
        select
            date(current_timestamp(), 'America/New_York') as snapshot_date,
            format_date(
                '%B %Y', date(current_timestamp(), 'America/New_York')
            ) as mbr_reference,
            case
                when s.months_since_completion = "Ongoing"
                then "Ongoing"
                when safe_cast(s.months_since_completion as int64) between 0 and 6
                then "0-6 MO. SINCE GRADUATION"
                when safe_cast(s.months_since_completion as int64) between 7 and 12
                then "7-12 MO. SINCE GRADUATION"
                when safe_cast(s.months_since_completion as int64) between 13 and 18
                then "13-18 MO. SINCE GRADUATION"
                when safe_cast(s.months_since_completion as int64) >= 19
                then "Concluded"
                else s.months_since_completion
            end as phase,
            s.client_program_name as program_name,
            s.program_code as program_code,
            s.country as country,
            s.start_date as start_date,
            s.end_date as end_date,
            s.months_since_completion as months_since_completion,
            s.admitted as admitted,
            s.enrolled_or_graduated as enrolled_or_graduated,
            s.dropped_out as dropped_out,
            s.placed_fellows as placed_fellows,
            s.beyond as beyond,
            s.direct_beyond as direct_beyond,
            s.indirect_beyond as indirect_beyond,
            s.internal_hire as internal_hire,
            s.external_qualifying as external_qualifying,
            s.internal_training as internal_training,
            air.sponsored_industry_certification as certification_name,
            s.program_revenue as program_revenue,
            s.cost_per_placement as cost_per_placement,
            s.cost_per_placement_without_beyond
            as cost_per_placement_without_beyond,
            s.program_nps as program_nps,
            s.placed_without_indirect as placed_without_indirect,
            s.beyond_without_indirect as beyond_without_indirect

        from {{ ref('gld_client_mbr__client_mbr_summary') }} s
        left join
            {{ ref('slv_airtable_programs__programs_dim') }} air 
            on air.program_code = s.program_code
    ),

    /* ---------------------------------------------------------------------
    3. MBR_SUMMARY_WITH_EU 
    Aggregates MBR summary data to create the 'EUR' country record.
    All output columns are snake_case.
    --------------------------------------------------------------------- */
    mbr_summary_with_eu as (
        select *
        from mbr_summary_base

        union all

        select
            snapshot_date,
            mbr_reference,
            phase,
            program_name,
            mb.program_code,
            'EUR' as country,
            max(start_date) as start_date,
            max(end_date) as end_date,
            max(months_since_completion) as months_since_completion,
            sum(admitted) as admitted,
            sum(enrolled_or_graduated) as enrolled_or_graduated,
            sum(dropped_out) as dropped_out,
            sum(placed_fellows) as placed_fellows,
            sum(beyond) as beyond,
            sum(coalesce(direct_beyond, 0)) as direct_beyond,
            sum(coalesce(indirect_beyond,0)) as indirect_beyond,
            sum(internal_hire) as internal_hire,
            sum(external_qualifying) as external_qualifying,
            sum(internal_training) as internal_training,
            max(certification_name) as certification_name,
            sum(coalesce(program_revenue, 0)) as program_revenue,

            -- calculate for EUR
            case
                when sum(coalesce(placed_fellows, 0)) > 0
                then
                    safe_divide(
                        sum(
                            coalesce(cost_per_placement, 0)
                            * coalesce(placed_fellows, 0)
                        ),
                        sum(coalesce(placed_fellows, 0))
                    )
                else avg(coalesce(cost_per_placement, null))
            end as cost_per_placement,

            case
                when sum(coalesce(placed_fellows, 0)) > 0
                then
                    safe_divide(
                        sum(
                            coalesce(cost_per_placement_without_beyond, 0)
                            * coalesce(placed_fellows, 0)
                        ),
                        sum(coalesce(placed_fellows, 0))
                    )
                else avg(coalesce(cost_per_placement_without_beyond, null))
            end as cost_per_placement_without_beyond,

            avg(coalesce(program_nps, null)) as program_nps,
            --
            sum(coalesce(placed_without_indirect, 0)) as placed_without_indirect,
            sum(coalesce(beyond_without_indirect, 0)) as beyond_without_indirect

        from mbr_summary_base mb
        join program_european_counts pec on mb.program_code = pec.program_code
        where
            mb.country in (select country_code from european_countries)
            and pec.num_european_countries > 1
        group by snapshot_date, mbr_reference, phase, program_name, program_code
    ),

    /* ---------------------------------------------------------------------
    4. CERT_UPDATES
    Fetches aggregated certification data for today's run.
    --------------------------------------------------------------------- */
    cert_updates as (
        select
            program_code,
            country,
            sum(coalesce(certification_attempted, 0)) as certification_attempted,
            sum(coalesce(certification_received, 0)) as certification_received,
            sum(coalesce(cert_eligible, 0)) as cert_eligible,
            sum(
                coalesce(job_transitions_with_certificate, 0)
            ) as job_transitions_with_certificate,
            sum(
                coalesce(external_qualifying_with_certificate, 0)
            ) as external_qualifying_with_certificate,
            sum(
                coalesce(internal_hire_with_certificate, 0)
            ) as internal_hire_with_certificate,
            sum(coalesce(beyond_with_certificate, 0)) as beyond_with_certificate,
            sum(
                coalesce(internal_training_with_certificate, 0)
            ) as internal_training_with_certificate,
            avg(coalesce(percent_exam_attempted, 0)) as percent_exam_attempted,
            avg(coalesce(percent_of_graduated, 0)) as percent_of_graduated,
            avg(
                coalesce(percent_of_job_transitions_with_certificate, 0)
            ) as percent_of_job_transitions_with_certificate

        from
           {{ ref('gld_client_mbr__certification_updates') }}
        group by program_code, country
    ),

    /* ---------------------------------------------------------------------
    5. CURRENT_MBR_DATA 
    Unites today's MBR summary with calculated rates and certification data.
    All output columns are snake_case.
    ---------------------------------------------------------------------*/
    current_mbr_data as (
        select
            summary.snapshot_date,
            summary.mbr_reference,
            summary.phase,
            summary.program_name,    
            summary.program_code,    
            summary.country,
            summary.start_date,        
            summary.end_date,            
            summary.months_since_completion, 
            summary.admitted,
            summary.enrolled_or_graduated as enrolled, 
            summary.dropped_out,
            summary.placed_fellows,
            summary.beyond,
            summary.direct_beyond,
            summary.indirect_beyond,
            summary.internal_hire,
            summary.external_qualifying,
            summary.internal_training,

            -- CALCULATED RATES
            safe_divide(
                summary.placed_fellows, nullif(summary.enrolled_or_graduated, 0)
            ) as placement_rate,
            safe_divide(
                summary.enrolled_or_graduated, nullif(summary.admitted, 0)
            ) as retention_rate,
        CASE 
            WHEN summary.months_since_completion = 'Ongoing' THEN NULL
            WHEN SAFE_CAST(summary.months_since_completion AS INT64) = 0 THEN NULL
            ELSE SAFE_DIVIDE(
                COALESCE(summary.placed_fellows),
                NULLIF(
                    (
                        COALESCE(summary.enrolled_or_graduated, 0) * COALESCE(SAFE_CAST(summary.months_since_completion AS INT64), 1)
                    ),
                    0
                )
            )
        END AS run_rate,

            summary.certification_name,

            coalesce(cu.certification_attempted, 0) as certification_attempted,
            coalesce(cu.certification_received, 0) as certification_received,
            coalesce(cu.percent_exam_attempted, 0) as percent_exam_attempted,
            coalesce(cu.percent_of_graduated, 0) as percent_of_graduated,
            coalesce(
                cu.job_transitions_with_certificate, 0
            ) as job_transitions_with_certificate,
            coalesce(
                cu.percent_of_job_transitions_with_certificate, 0
            ) as percent_of_job_transitions_with_certificate,
            coalesce(
                cu.external_qualifying_with_certificate, 0
            ) as external_qualifying_with_certificate,
            coalesce(
                cu.internal_hire_with_certificate, 0
            ) as internal_hire_with_certificate,
            coalesce(cu.beyond_with_certificate, 0) as beyond_with_certificate,
            coalesce(
                cu.internal_training_with_certificate, 0
            ) as internal_training_with_certificate,
            coalesce(cu.cert_eligible, 0) as cert_eligible,

            summary.program_revenue,
            summary.cost_per_placement,
            summary.cost_per_placement_without_beyond,
            summary.program_nps,
            summary.placed_without_indirect,
            summary.beyond_without_indirect,
            timestamp(datetime(current_timestamp(), "America/New_York")) as last_snapshot_ts_est
        from mbr_summary_with_eu summary
        left join
            cert_updates cu
            on summary.program_code = cu.program_code
            and summary.country = cu.country
    ),


    /* ---------------------------------------------------------------------
    6. FULL_HISTORY_DATA
    Unions the filtered history (M-1) with the current day's calculated data (M).
    All columns are already snake_case.
    --------------------------------------------------------------------- */
    full_history_data as (
        select *, parse_date('%B %Y', mbr_reference) as sort_date
        from
            (
                -- Reading the history (already snake_case from filtered_history_for_lag)
                select * from filtered_history_for_lag
{% if is_incremental() %}
                -- Reading the current day's data (already aliased to snake_case)
                union all
                select * from current_mbr_data
{% endif %}
            )
    ),

    /* ---------------------------------------------------------------------
    7. WITH_LAG_DATA
    Preserves the final schema input rows.
    --------------------------------------------------------------------- */
    with_lag_data as (
        select f.*
        from full_history_data as f
    )

-- -------------------------------------------------------------------
-- FINAL SELECT - Maps to the final schema (snake_case) and calculates final MoM metrics
-- ---------------------------------------------------------------------
select
    -- We select current_date() here to ensure the latest execution date is captured
    COALESCE(final.snapshot_date,current_date('America/New_York')) as snapshot_date,
    final.mbr_reference as mbr_reference,
    final.phase as phase,
    
    -- Using snake_case aliases to map back to the desired final schema names
    final.program_name as program_name,
    final.program_code as program_code,
    final.country as country,
    final.start_date as start_date,
    final.end_date as end_date,
    final.months_since_completion as months_since_completion,
    final.admitted as admitted,
    
    -- Core Metrics
    final.enrolled_or_graduated as enrolled_or_graduated,
    final.dropped_out as dropped_out,
    final.placed_fellows as placed_fellows,
    final.beyond as beyond,
    final.direct_beyond as direct_beyond,
    final.indirect_beyond as indirect_beyond,
    final.internal_hire as internal_hire,
    final.external_qualifying as external_qualifying,
    final.internal_training as internal_training,
    
    -- Rates
    final.placement_rate as placement_rate,
    final.retention_rate as retention_rate,
    final.run_rate as run_rate,

    -- certificate data 
    final.certification_name as certification_name,
    final.certification_attempted as certification_attempted,
    final.certification_received as certification_received,
    final.percent_exam_attempted as percent_exam_attempted,
    final.percent_of_graduated as percent_of_graduated,
    final.job_transitions_with_certificate as job_transitions_with_certificate,
    final.percent_of_job_transitions_with_certificate
    as percent_of_job_transitions_with_certificate,
    final.external_qualifying_with_certificate
    as external_qualifying_with_certificate,
    final.internal_hire_with_certificate as internal_hire_with_certificate,
    final.beyond_with_certificate as beyond_with_certificate,
    final.internal_training_with_certificate as internal_training_with_certificate,
    final.cert_eligible as cert_eligible,
    final.program_revenue as program_revenue,
    final.cost_per_placement as cost_per_placement,
    final.cost_per_placement_without_beyond as cost_per_placement_without_beyond,
    final.program_nps as program_nps,
    final.placed_without_indirect as placed_without_indirect,
    final.beyond_without_indirect as beyond_without_indirect,
    final.last_snapshot_ts_est as last_snapshot_ts_est

from with_lag_data final

where 1=1 
{% if is_incremental() %}
    AND final.snapshot_date = current_date('America/New_York')
    AND (
        -- allow override if needed (bypasses all checks)
        {{ run_override }} = true
        OR
        (
            -- Only run on friday (6) or last day of the month 
            (
                EXTRACT(DAYOFWEEK FROM current_date('America/New_York')) = 6
                OR 
                current_date('America/New_York') = LAST_DAY(current_date('America/New_York'))
            )
            -- AND there's no snapshot already for the current date
            AND NOT EXISTS (
                SELECT snapshot_date
                FROM {{ this }} 
                WHERE snapshot_date = current_date('America/New_York')
            )
        )
    )
{% endif %}