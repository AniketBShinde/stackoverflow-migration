{{ config(
    materialized='incremental',
    engine='MergeTree()',
    order_by='post_id',
    unique_key='post_id'
) }}

SELECT
    id AS post_id,
    title,
    toDateTime64(creation_date, 3) AS created_at,
    score,
    view_count,
    comment_count,
    assumeNotNull(
        splitByChar(',', replaceRegexpAll(ifnull(tags, ''), '[\\[\\]\\s\']', ''))
    ) AS tags
FROM {{ source('stackoverflow_raw', 'stackoverflow_final') }}

{% if is_incremental() %}
  -- Look back 7 days from the current max date in the table
  -- This ensures if scores/views changed in the last week, we update them
  WHERE created_at >= (SELECT max(created_at) - INTERVAL 5 DAY FROM {{ this }})
{% endif %}