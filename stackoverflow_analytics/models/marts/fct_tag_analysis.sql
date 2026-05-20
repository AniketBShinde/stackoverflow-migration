{{ config(
    materialized='table',
    engine='SummingMergeTree()',
    order_by='(created_date, tag)'
) }}

SELECT
    toDate(created_at) AS created_date,
    arrayJoin(tags) AS tag,
    count() AS post_count,
    sum(view_count) AS total_views,
    avg(score) AS avg_score
FROM {{ ref('stg_posts') }}
-- Filter out empty tags
WHERE tag != ''
GROUP BY 
    created_date, 
    tag