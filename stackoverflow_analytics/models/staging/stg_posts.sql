{{ config(
    materialized='incremental',
    engine='MergeTree()',
    order_by='post_id',
    unique_key='post_id'
) }}

SELECT
    assumeNotNull(id) AS post_id,
    title,
    toDateTime64(assumeNotNull(creation_date), 3) AS created_at,
    score,
    view_count,
    comment_count,
    assumeNotNull(
        splitByChar(',', replaceRegexpAll(ifnull(tags, ''), '[\\[\\]\\s\']', ''))
    ) AS tags
FROM {{ source('stackoverflow_raw', 'stackoverflow_final') }}
WHERE id IS NOT NULL
  AND creation_date IS NOT NULL

{% if is_incremental() %}
  -- Look back 5 days from the current max date in the table
  -- This ensures if scores/views changed in the last week, we update them
  AND creation_date >= (SELECT max(created_at) - INTERVAL 5 DAY FROM {{ this }})
{% endif %}