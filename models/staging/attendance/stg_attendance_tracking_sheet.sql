{{ config(materialized="ephemeral") }}

WITH alt_user_service as (

    SELECT 
        primary_user_service_id,
        primary_alt_account_user_service_id,
        primary_id_email,
        alt_account_email,
    FROM 
        {{ ref('slv_user_service__user_alt_accounts_mapping_dim') }}

),

get_us_linking as (

    SELECT 
        primary_user_service_id,
        user_service_id,
    FROM {{ ref('slv_user_service__user_linked_mapping_dim') }}

),

meetings_fact as (

    SELECT 
        meeting_id,
        meeting_instance_id,
        primary_user_service_id,
        participant_email,
        participant_name,
        participant_meeting_instance_id,
        join_time,
        leave_time
    FROM {{ ref('slv_attendance__participant_meeting_instance_fact') }}
    WHERE 
        duration IS NOT NULL
        AND NOT REGEXP_CONTAINS(
                    LOWER(participant_email),
                    r'@company\.com\b|\bcompany\b.*\btest\b|\btest\b.*\bcompany\b|\+test\d*@|@internal\.com\b'
          )  
    AND is_infered_optional_meeting = FALSE
),

participant_intervals AS (

    SELECT DISTINCT
        meeting_id,
        meeting_instance_id,
        primary_user_service_id,
        participant_email,
        participant_name,
        participant_meeting_instance_id,
        join_time,
        leave_time
    FROM meetings_fact
    WHERE
        primary_user_service_id IS NOT NULL

),

-- An user can login in multiple devices, we just want to consider the total interval
-- when they intersect, so taking the earlier join time and the later leave time
overlapping_intervals AS (

    SELECT
        a.meeting_id,
        a.meeting_instance_id,
        a.primary_user_service_id,
        a.participant_email,
        a.participant_name,
        LEAST(MIN(a.join_time), MIN(b.join_time)) AS earliest_join_time,
        GREATEST(MAX(a.leave_time), MAX(b.leave_time)) AS latest_leave_time
    FROM participant_intervals AS a
    INNER JOIN participant_intervals AS b
        ON
            a.primary_user_service_id = b.primary_user_service_id
            AND a.meeting_id = b.meeting_id
            AND a.meeting_instance_id = b.meeting_instance_id
            AND a.join_time <= b.join_time
            AND a.leave_time >= b.join_time
            -- Exclude self join
            AND a.participant_meeting_instance_id
            != b.participant_meeting_instance_id
    GROUP BY a.meeting_id, a.meeting_instance_id, a.primary_user_service_id, a.participant_email, a.participant_name

),

non_overlapping_intervals AS (

    SELECT pi.*
    FROM participant_intervals AS pi
    LEFT JOIN overlapping_intervals AS oi
        ON
            pi.meeting_id = oi.meeting_id
            AND pi.meeting_instance_id = oi.meeting_instance_id
            AND pi.primary_user_service_id = oi.primary_user_service_id
    WHERE (
        oi.meeting_id IS NOT null
        AND (
            pi.join_time
            < oi.earliest_join_time
            OR pi.leave_time
            > oi.latest_leave_time
        )
    ) OR oi.meeting_id IS null

),

combined_intervals AS (

    SELECT
        meeting_id,
        meeting_instance_id,
        primary_user_service_id,
        participant_email,
        participant_name,
        earliest_join_time AS join_time,
        latest_leave_time AS leave_time
    FROM overlapping_intervals
    UNION ALL
    SELECT
        meeting_id,
        meeting_instance_id,
        primary_user_service_id,
        participant_email,
        participant_name,
        join_time,
        leave_time
    FROM non_overlapping_intervals

),

attendance_summary AS (

    SELECT
        ci.meeting_id,
        ci.meeting_instance_id,
        ci.primary_user_service_id,
        ci.participant_email,
        ci.participant_name,
        TIMESTAMP_DIFF(ci.leave_time, ci.join_time, SECOND) AS duration
    FROM combined_intervals AS ci

),

compute_attendance as (

    SELECT
        md.program_code,
        sd.meeting_id,
        sd.meeting_instance_id,
        md.lesson_id,
        md.lesson_title,
        sd.primary_user_service_id,
        sd.participant_email,
        sd.participant_name,
        md.start_time_ts AS meeting_start_time_ts,
        md.end_time_ts AS meeting_end_time_ts,
        md.meeting_duration_seconds,
        SUM(sd.duration) AS attendance_duration_seconds,
        SUM(sd.duration) / NULLIF(md.meeting_duration_seconds, 0) AS attendance_percentage,
        IF(
            SUM(sd.duration) > 0.5 * md.meeting_duration_seconds,
            true,
            false
        ) AS has_participant_attended_flag,
    FROM attendance_summary AS sd
    INNER JOIN
        {{ ref('slv_attendance__meeting_instance_dim') }} AS md
        ON sd.meeting_instance_id = md.meeting_instance_id
        AND md.is_infered_optional_meeting = FALSE
    GROUP BY
        md.program_code,
        sd.meeting_id,
        sd.meeting_instance_id,
        md.lesson_id,
        md.lesson_title,
        sd.primary_user_service_id,
        sd.participant_email,
        sd.participant_name,
        md.start_time_ts,
        md.end_time_ts,
        md.meeting_duration_seconds

),

get_users as (

    SELECT DISTINCT 
        link.primary_user_service_id,
        learn.user_service_id as user_service_id_in_program_service, 
        learn.email as participant_email,
        concat(learn.first_name,' ',learn.last_name) as participant_name,
        learn.program_code,
        learn.enrollments_status,
    FROM {{ ref('slv_program_service__learner_dim') }} as learn
    INNER JOIN get_us_linking as link 
        ON learn.user_service_id = link.user_service_id
    WHERE learn.edw_is_current AND learn.edw_is_deleted IS false

),

get_lessons as (

    SELECT DISTINCT
        lessons.lesson_id,
        lessons.lesson_title,
        COALESCE(lxd.delivery_ts, lessons.lesson_start_date_ts) AS lesson_start_date_ts,
        COALESCE(lxd.lecture_end_ts, lessons.lesson_end_date_ts) AS lesson_end_date_ts,
        lessons.program_code
    FROM 
        {{ ref('slv_program_service__lectures_dim') }} as lessons
    LEFT JOIN {{ ref('slv_airtable_content_ops__program_content_dim') }} as lxd
        ON lessons.lesson_title = lxd.lesson_event AND lessons.program_code = lxd.program_code
    WHERE lessons.edw_is_current is true AND lessons.edw_is_deleted is false
    and lessons.lesson_title is not null


),

user_lesson_grid as (

    SELECT
        u.primary_user_service_id,
        u.user_service_id_in_program_service,
        u.participant_email,
        u.participant_name,
        u.program_code,
        u.enrollments_status,
        l.lesson_id,
        l.lesson_title,
        l.lesson_start_date_ts,
        l.lesson_end_date_ts,
    FROM get_users   AS u
    JOIN get_lessons AS l
      USING (program_code)  

),


apply_attendance_to_user_lesson_grid as (

    SELECT DISTINCT
        lessons.program_code,
        coalesce(compute.meeting_id, compute_alt.meeting_id) as meeting_id,
        coalesce(compute.meeting_instance_id, compute_alt.meeting_instance_id) as meeting_instance_id,
        lessons.lesson_id,
        lessons.lesson_title,
        lessons.lesson_start_date_ts,
        lessons.lesson_end_date_ts,
        lessons.primary_user_service_id,
        lessons.user_service_id_in_program_service,
        lessons.participant_email,
        lessons.participant_name,
        lessons.enrollments_status,
        coalesce(compute.meeting_start_time_ts, compute_alt.meeting_start_time_ts) as meeting_start_time_ts,
        coalesce(compute.meeting_end_time_ts, compute_alt.meeting_end_time_ts) as meeting_end_time_ts,
        coalesce(compute.meeting_duration_seconds, compute_alt.meeting_duration_seconds) as meeting_duration_seconds,
        coalesce(compute.attendance_duration_seconds, compute_alt.attendance_duration_seconds) as attendance_duration_seconds,
        coalesce(compute.attendance_percentage, compute_alt.attendance_percentage) as attendance_percentage,
        coalesce(compute.has_participant_attended_flag, compute_alt.has_participant_attended_flag) as has_participant_attended_flag,
    FROM 
        user_lesson_grid as lessons 
    LEFT JOIN compute_attendance as compute
        ON lessons.lesson_id = compute.lesson_id
        AND lessons.program_code = compute.program_code
        AND lessons.primary_user_service_id = compute.primary_user_service_id
    -- if the zoom account does not match their program service account
    -- this will match them anyways for Ops
    LEFT JOIN alt_user_service as alt 
        ON lessons.primary_user_service_id = alt.primary_user_service_id
        AND alt.primary_alt_account_user_service_id IS NOT NULL
    LEFT JOIN compute_attendance as compute_alt
        ON alt.primary_user_service_id = compute_alt.primary_user_service_id
        AND lessons.lesson_id = compute_alt.lesson_id
        AND lessons.program_code = compute_alt.program_code

),


excused_attendance as (


    SELECT 
        program_code,
        excused_attendance_date,
        submitted_at,
        excuse_reason,
        email,
        first_name,
        last_name,
    FROM {{ ref('stg_attendance__excused_absences') }}
    -- older forms that were not standardized have dates like 'Thursday' which means they are converted to null
    WHERE excused_attendance_date IS NOT NULL AND email IS NOT NULL
    -- some people submitted a form to be excused for the same day multiple times
    -- so we just take their most recent
    -- qualify is fine since this allows for less SQL and the total historical number of excused absence submissions
    -- are like 5000, so being slight inefficient is fine here to allow for smaller more concise code
    QUALIFY ROW_NUMBER() OVER(PARTITION BY form_id,email,excused_attendance_date ORDER BY submitted_at DESC) = 1


),

handle_excused_attendance as (

    SELECT DISTINCT
        grid.program_code,
        grid.meeting_id,
        grid.meeting_instance_id,
        grid.lesson_id,
        grid.lesson_title,
        grid.lesson_start_date_ts,
        grid.lesson_end_date_ts,
        grid.primary_user_service_id,
        grid.user_service_id_in_program_service,
        grid.participant_email,
        grid.participant_name,
        grid.enrollments_status,
        grid.meeting_start_time_ts,
        grid.meeting_end_time_ts,
        grid.meeting_duration_seconds,
        grid.attendance_duration_seconds,
        grid.attendance_percentage,
        grid.has_participant_attended_flag,
        CASE 
            WHEN excuse.email IS NOT NULL THEN TRUE
            WHEN alt_excuse_pri_email.email IS NOT NULL THEN TRUE
            WHEN alt_excuse_alt_email.email IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS has_excused_attendance,
        date(
            COALESCE(
                excuse.submitted_at,
                alt_excuse_pri_email.submitted_at,
                alt_excuse_alt_email.submitted_at
            )
        ) as excuse_submitted_date,
        COALESCE(
            excuse.excuse_reason,
            alt_excuse_pri_email.excuse_reason,
            alt_excuse_alt_email.excuse_reason
        ) as excuse_reason,
    FROM 
        apply_attendance_to_user_lesson_grid as grid
    LEFT JOIN excused_attendance as excuse 
        ON grid.participant_email = excuse.email
        AND date(grid.lesson_start_date_ts) = excuse.excused_attendance_date
        AND grid.program_code = excuse.program_code
    LEFT JOIN alt_user_service as alt_id
        ON grid.primary_user_service_id = alt_id.primary_user_service_id
    LEFT JOIN excused_attendance as alt_excuse_pri_email
        ON alt_id.primary_id_email = alt_excuse_pri_email.email
        AND date(grid.lesson_start_date_ts) = alt_excuse_pri_email.excused_attendance_date
        AND grid.program_code = alt_excuse_pri_email.program_code
    LEFT JOIN excused_attendance as alt_excuse_alt_email
        ON alt_id.alt_account_email = alt_excuse_alt_email.email
        AND date(grid.lesson_start_date_ts) = alt_excuse_alt_email.excused_attendance_date
        AND grid.program_code = alt_excuse_alt_email.program_code

),

primary_us_learner_activity as (

    SELECT 
        link.primary_user_service_id,
        activity.program_id,
        CAST(JSON_VALUE(activity.overrides,'$.is_required') as boolean) as is_required,
        activity.activity_id as lesson_id,
    FROM 
        {{ ref('slv_grading__learner_activity_fact') }} as activity
    INNER JOIN get_us_linking as link 
        ON activity.learner_id = link.primary_user_service_id
    WHERE activity.activity_type = 'lesson'

),

handle_learner_pathways as (

    SELECT DISTINCT
        excuse.program_code,
        excuse.meeting_id,
        excuse.meeting_instance_id,
        excuse.lesson_id,
        excuse.lesson_title,
        excuse.lesson_start_date_ts,
        excuse.lesson_end_date_ts,
        excuse.primary_user_service_id,
        excuse.user_service_id_in_program_service,
        excuse.participant_email,
        excuse.participant_name,
        excuse.enrollments_status,
        excuse.meeting_start_time_ts,
        excuse.meeting_end_time_ts,    
        excuse.meeting_duration_seconds,
        excuse.attendance_duration_seconds,
        excuse.attendance_percentage,
        excuse.has_participant_attended_flag,
        excuse.has_excused_attendance,
        excuse.excuse_submitted_date,
        excuse.excuse_reason,
        CASE WHEN activity.is_required = FALSE then TRUE else FALSE END as is_exempt,
        CURRENT_TIMESTAMP() as edw_created_at,
    FROM 
        handle_excused_attendance as excuse 
    LEFT JOIN primary_us_learner_activity as activity 
        ON excuse.primary_user_service_id = activity.primary_user_service_id
        AND excuse.program_code = activity.program_id
        AND excuse.lesson_id = activity.lesson_id

)


SELECT * FROM handle_learner_pathways