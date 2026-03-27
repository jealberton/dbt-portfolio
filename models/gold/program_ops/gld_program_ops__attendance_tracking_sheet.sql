{%- set data_layer = 'gold' %}
{%- set data_source = 'program_operations' %}
{%- set table_name = get_table_name() %}
{%- set labels = get_labels(
  labels=['global',data_layer],
  custom_labels={
    'automated_by': 'data-engineer',
    'data_classification': 'private',
    'data_domain': 'operations',
    'data_source': data_source,
    'environment': target.target_name,
  }
)%}

{{
  config(
    alias = table_name,
    labels = labels,
    tags = ['attendance', 'gold__attendance'],
    cluster_by = ['program_code','lesson_title']
  )
}}

select * from (
  -- the weeks cte have the purpose of generating a "list of weeks" based on the program start and end dates from airtable
  -- to use in other programs we can remove the program_code filter
WITH weeks AS (
  SELECT 
    program_code,
    DATE_TRUNC(DATE(program_start_ts), WEEK(MONDAY)) + INTERVAL n WEEK AS week_start_date,
    DATE_TRUNC(DATE(program_start_ts), WEEK(MONDAY)) + INTERVAL n WEEK + INTERVAL 6 DAY AS week_end_date,
    n + 1 AS week_number
  FROM (
    SELECT 
      program_code, 
      program_start_ts,
      program_end_ts,
      GENERATE_ARRAY(
        0, 
        DATE_DIFF(DATE_TRUNC(DATE(program_end_ts), WEEK(MONDAY)), DATE_TRUNC(DATE(program_start_ts), WEEK(MONDAY)), WEEK(MONDAY))
      ) AS weeks_array
    FROM {{ ref('slv_airtable_programs__programs_dim') }}
  ), UNNEST(weeks_array) AS n
),

-- the cte partipants gather the list of enrolled learners. The program_start_ts from airtable also will be use further here
participants AS (
  SELECT DISTINCT 
    a.user_service_id as primary_user_service_id, 
    CONCAT(a.first_name,' ',a.last_name) as participant_name, 
    a.email as participant_email, 
    a.program_code,
    p.program_start_ts
  FROM {{ ref('slv_program_service__learner_dim') }} a
  JOIN {{ ref('slv_airtable_programs__programs_dim') }} p
    ON a.program_code = p.program_code
  WHERE a.enrollments_status = 'ENROLLED'
  AND a.edw_is_current is true AND a.edw_is_deleted is false
),

-- attendance_raw brings the attendance data from the original attendance_tracking_sheet
attendance_raw AS (
  SELECT DISTINCT
    *,
    DATE(COALESCE(DATE(lesson_start_date_ts), DATE(meeting_start_time_ts))) AS lesson_date,
    FORMAT_TIMESTAMP('%A', COALESCE(DATE(lesson_start_date_ts), DATE(meeting_start_time_ts))) AS day_of_week
  FROM {{ ref('stg_attendance_tracking_sheet') }}
  WHERE
  (DATE(lesson_start_date_ts) is null OR DATE(lesson_start_date_ts) <= CURRENT_DATE()) 
),

--formatting day of week
lessons_by_week AS (
  SELECT DISTINCT 
    program_code,
    DATE(COALESCE(DATE(lesson_start_date_ts), DATE(meeting_start_time_ts))) AS lesson_date,
    FORMAT_TIMESTAMP('%A', COALESCE(DATE(lesson_start_date_ts), DATE(meeting_start_time_ts))) AS day_of_week,
    lesson_title
  FROM attendance_raw
  WHERE lesson_title IS NOT NULL
),

-- CTE to generate all participant/lessons possible combinations
participant_lesson_combinations AS (
  SELECT DISTINCT
    p.program_code,
    p.primary_user_service_id,
    p.participant_name,
    p.participant_email,
    l.lesson_title,
    l.day_of_week,
    l.lesson_date,
    -- this case/when maps when a session happened before the oficial program start date. Sometimes we can track the attendance for those sessions, sometimes not
    CASE 
      WHEN DATE(l.lesson_date) < DATE_TRUNC(DATE(p.program_start_ts), WEEK(MONDAY)) THEN 'pre-program start'
      ELSE CAST(w.week_number AS STRING)
    END AS week_number,
    -- this case/when formats the week_range
    CASE 
      WHEN DATE(l.lesson_date) < DATE_TRUNC(DATE(p.program_start_ts), WEEK(MONDAY)) THEN 'pre-program start'
      ELSE FORMAT_DATE('%b %d', w.week_start_date) || ' - ' || FORMAT_DATE('%b %d', w.week_end_date)
    END AS week_range
  FROM participants p
  JOIN lessons_by_week l ON p.program_code = l.program_code
  LEFT JOIN weeks w ON 
    p.program_code = w.program_code AND
    DATE(l.lesson_date) BETWEEN w.week_start_date AND w.week_end_date
),

-- the waived_form cte is used to create the waived_attendance flag according to the new waived form. Waived attendance is when the learner is dismiss from lessons for a extended period of time,
--different from excused when is a particular abscence
waived_form AS (
  SELECT
    *,
    day AS waived_day
  FROM (
    SELECT
      r.form_id,
      r.submitted_at,
      MAX(CASE WHEN q.question_id = 'QUESTION_ID_1' THEN r.response END) AS program_code,
      MAX(CASE WHEN q.question_id = 'QUESTION_ID_2' THEN r.response END) AS learner_email,
      MAX(CASE WHEN q.question_id = 'QUESTION_ID_3' THEN r.response END) AS waived_class_day,
      DATE(MAX(CASE WHEN q.question_id = 'QUESTION_ID_4' THEN r.response END)) AS waived_period_start,
      DATE(MAX(CASE WHEN q.question_id = 'QUESTION_ID_5' THEN r.response END)) AS waived_period_end,
      MAX(CASE WHEN q.question_id = 'QUESTION_ID_6' THEN r.response END) AS waived_reason
    FROM {{ ref('slv_typeform_api_question_summary_forms_fact') }} q
    JOIN {{ ref('slv_typeform_response_detail_fact') }} r 
      ON LOWER(r.question_id) = LOWER(q.question_id)
      AND LOWER(r.form_id) = LOWER(q.form_id)
    WHERE UPPER(r.form_id) IN ('FORM_ID_001')
    GROUP BY r.form_id, r.submitted_at
  ),
  UNNEST(GENERATE_DATE_ARRAY(
    SAFE_CAST(waived_period_start AS DATE), 
    SAFE_CAST(waived_period_end AS DATE))) AS day
),

alt_user_service AS (
    SELECT 
        primary_user_service_id,
        primary_id_email,
        alt_account_email
    FROM 
        {{ ref('slv_user_service__user_alt_accounts_mapping_dim') }}
),

-- CTE agg attendance
--Aggregating attendance for the final output
attendance_aggregated AS (
  SELECT
    plc.program_code,
    plc.week_number,
    plc.week_range,
    plc.primary_user_service_id,
    plc.participant_name,
    plc.participant_email,
    plc.lesson_title,
    plc.day_of_week,
    MAX(CASE WHEN a.primary_user_service_id IS NOT NULL THEN a.has_participant_attended_flag ELSE NULL END) AS has_participant_attended_flag,
    MAX(CASE WHEN a.primary_user_service_id IS NOT NULL THEN a.has_excused_attendance ELSE NULL END) AS has_excused_attendance,
    MAX(CASE WHEN a.primary_user_service_id IS NOT NULL THEN a.is_exempt ELSE NULL END) AS is_exempt,
    MAX(CASE WHEN a.primary_user_service_id IS NOT NULL THEN a.attendance_percentage ELSE NULL END) AS attendance_percentage,
    MAX(CASE WHEN wf.learner_email IS NOT NULL THEN TRUE ELSE FALSE END) AS has_waived_attendance
  FROM participant_lesson_combinations plc
  LEFT JOIN attendance_raw a ON
    a.program_code = plc.program_code AND
    a.primary_user_service_id = plc.primary_user_service_id AND
    a.lesson_title = plc.lesson_title AND
    a.day_of_week = plc.day_of_week
  LEFT JOIN alt_user_service alt
    ON plc.primary_user_service_id = alt.primary_user_service_id
  LEFT JOIN waived_form wf ON
    wf.waived_day = plc.lesson_date
    AND (
        LOWER(TRIM(wf.learner_email)) = LOWER(TRIM(plc.participant_email)) OR
        LOWER(TRIM(wf.learner_email)) = LOWER(TRIM(alt.primary_id_email)) OR
        LOWER(TRIM(wf.learner_email)) = LOWER(TRIM(alt.alt_account_email))
    )
  GROUP BY
    plc.program_code,
    plc.week_number,
    plc.week_range,
    plc.primary_user_service_id,
    plc.participant_name,
    plc.participant_email,
    plc.lesson_title,
    plc.day_of_week
),

fixed_exempts as ( --capturing exempts properly. 

-- Exempt is when a learner already passed all the assessments and is exempt from future lessons 
-- There was an issue with the is_exempt flag comming from the gold_attendance_tracking_sheet at the time, it wasn't working properly.
-- Instead of "is_required" we need to shift for "is_optional" to create the condition. Since this maybe might change in the future, the stg_attendace_tracking_sheet keeps as it is for now
  with lessons as (
   SELECT DISTINCT
        lessons.lesson_id,
        lessons.lesson_title,
        lxd.category,
        COALESCE(DATE(lxd.delivery_ts),DATE(lessons.lesson_start_date_ts)) as lesson_start_date_ts,
        COALESCE(DATE(lxd.lecture_end_ts),DATE(lessons.lesson_end_date_ts)) as lesson_end_date_ts,
        lessons.program_code
    FROM 
        {{ ref('slv_program_service__lectures_dim') }} as lessons
    LEFT JOIN {{ ref('slv_airtable_content_ops__program_content_dim') }} as lxd
        ON lessons.lesson_title = lxd.lesson_event and lessons.program_code = lxd.program_code
    WHERE lessons.edw_is_current is true AND lessons.edw_is_deleted is false
    and lessons.lesson_title is not null
),

overrides as (
select 
a.*,
JSON_EXTRACT_SCALAR(overrides, '$.optional') AS optional,
JSON_EXTRACT_SCALAR(overrides, '$.is_required') AS is_required
from {{ ref('slv_grading__learner_activity_fact') }} a
where activity_type ='lesson'
)

select distinct
o.learner_id, 
o.optional,
l.lesson_title,
l.lesson_start_date_ts
from overrides o join lessons l 
on activity_id = lesson_id 
where optional = 'true' 
order by learner_id, lesson_start_date_ts
),

-- CTE final pivot
final_attendance AS (
  SELECT
    ag.program_code,
    ag.week_number,
    ag.week_range,
    ag.primary_user_service_id,
    ag.participant_name,
    ag.participant_email,
    ag.day_of_week,
    STRING_AGG(DISTINCT ag.lesson_title, ' / ') AS lesson_title,
    
    --attendance
    MAX(
      CASE 
        WHEN fe.optional ='true' THEN 'exempted'
        WHEN ag.has_excused_attendance THEN 'excused'
        WHEN ag.has_participant_attended_flag THEN 'attended'
        WHEN ag.has_participant_attended_flag IS FALSE or ag.has_participant_attended_flag IS NULL THEN 'absent'
        ELSE '' 
      END)
     AS attendance,
    
       
    -- Other fields
    MAX(has_excused_attendance) AS has_excused_attendance,
    CASE WHEN MAX(fe.optional)='true' THEN 'true' ELSE 'false' END AS is_exempt,
    MAX(attendance_percentage) AS attendance_percentage,
    MAX(has_waived_attendance) AS has_waived_attendance
  FROM attendance_aggregated ag
  LEFT JOIN fixed_exempts fe ON fe.learner_id = primary_user_service_id AND ag.lesson_title LIKE CONCAT('%',fe.lesson_title,'%')
  GROUP BY
    ag.program_code,
    ag.week_number,
    ag.week_range,
    ag.primary_user_service_id,
    ag.participant_name,
    ag.participant_email,
    ag.day_of_week
),
 upcoming_weeks_participants AS (
  SELECT
    p.program_code,
    p.primary_user_service_id,
    p.participant_name,
    p.participant_email,
    CAST(w.week_number AS STRING) AS week_number,
    FORMAT_DATE('%b %d', w.week_start_date) || ' - ' || FORMAT_DATE('%b %d', w.week_end_date) AS week_range
  FROM participants p
  JOIN weeks w 
    ON p.program_code = w.program_code
  WHERE w.week_start_date > CURRENT_DATE() -- future weeks
)

--final select

SELECT DISTINCT
  program_code,
  week_number,
  week_range,
  primary_user_service_id,
  participant_name,
  participant_email,
  lesson_title, 
  attendance as week_attendance,
  has_excused_attendance,
  cast(is_exempt as boolean) as is_exempt,
  has_waived_attendance,
  attendance_percentage,
  --- overall_status with some extra conditions
  CASE 
    WHEN lower(lesson_title) like '%break%' then 'break'
    WHEN is_exempt = 'true' THEN 'exempted'
    WHEN has_excused_attendance = true THEN 'excused'
    WHEN has_waived_attendance THEN 'waived'
    WHEN attendance = 'attended' THEN 'attended'
    WHEN attendance_percentage <0.5 THEN 'attended (attendance percentage under 50%)'
    ELSE 'absent'
  END AS attendance_overall_status
FROM final_attendance

UNION ALL 

SELECT 
  program_code,
  week_number,
  week_range,
  primary_user_service_id,
  participant_name,
  participant_email,
  NULL AS lesson_title,
  NULL AS attendance,
  NULL AS has_excused_attendance,
  NULL AS is_exempt,
  NULL AS has_waived_attendance,
  NULL AS attendance_percentage,
  'upcoming lesson' AS attendance_overall_status
FROM upcoming_weeks_participants 
ORDER BY 
  CASE 
    WHEN week_number = 'pre-program start' THEN 0
    ELSE SAFE_CAST(week_number AS INT64)
  END

) 
where week_number is not null